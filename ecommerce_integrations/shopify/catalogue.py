"""Shopify Catalogue page: ERPNext products side by side with Shopify.

One row per product as Shopify sees it: a template item (its variants become
Shopify variants) or a simple item. Stock is summed over the warehouses mapped
to Shopify locations (or one of them, when the store filter is set). Shopify's
own state (status, publication) comes from the Shopify Catalogue Product copy,
refreshed hourly and after every upload, so the page never waits on Shopify.
"""

import csv
import io
import json
import time

import frappe
from frappe import _
from frappe.utils import cint, cstr, flt, get_datetime, now
from shopify.collection import PaginatedIterator
from shopify.resources import Product, Variant

from ecommerce_integrations.shopify.connection import temp_shopify_session
from ecommerce_integrations.shopify.constants import (
	ITEM_SELLING_RATE_FIELD,
	MODULE_NAME,
	SETTING_DOCTYPE,
)

SNAPSHOT = "Shopify Catalogue Product"
ROLE = "System Manager"
PAGE_LENGTH = 50
EXPORT_LIMIT = 20000
BULK_LIMIT = 5000
MAX_VARIANTS = 100  # Shopify REST limit per product

JOB_KEY = "shopify_catalogue:job"
JOB_NAME = "shopify.catalogue.job"
REFRESH_JOB_NAME = "shopify.catalogue.refresh"
REFRESHED_KEY = "shopify_catalogue:refreshed_at"
RETRY_METHOD = "ecommerce_integrations.shopify.product.retry_item_upload"

# Sidebar sections. Each is a condition on the product rollup `p`.
BUCKETS = {
	"all": "1 = 1",
	"not_in_shopify": "p.product_id is null and p.disabled = 0 and p.stock > 0",
	"partial": "p.product_id is not null and p.disabled = 0 and p.unlinked_in_stock > 0",
	"missing_price": "p.disabled = 0 and p.unpriced > 0 and (p.stock > 0 or p.product_id is not null)",
	"pending": "p.pending > 0",
	"disabled_live": "p.disabled = 1 and p.shopify_status = 'active'",
	"in_shopify": "p.product_id is not null",
	"not_published": "p.product_id is not null and p.shopify_status is not null"
	" and (p.shopify_status <> 'active' or p.published = 0)",
	"no_stock": "p.product_id is not null and p.stock <= 0 and p.shopify_status = 'active'",
}

SORTS = {
	"stock": "p.stock desc, p.item_name asc",
	"name": "p.item_name asc",
	"newest": "p.creation desc",
	"price": "p.max_rate desc, p.item_name asc",
}


class SkipProduct(Exception):
	"""The product can't be sent as it is (no price, disabled...): report, go on."""


# ---------------------------------------------------------------------------
# Reading
# ---------------------------------------------------------------------------


def _check_access():
	frappe.only_for(ROLE)


def _rate_column() -> str:
	return ITEM_SELLING_RATE_FIELD if frappe.db.has_column("Item", ITEM_SELLING_RATE_FIELD) else None


def _product_sql() -> str:
	rate = _rate_column()
	rate_i = f"ifnull(i.`{rate}`, 0)" if rate else "0"
	rate_v = f"ifnull(v.`{rate}`, 0)" if rate else "0"
	gender = "i.gender" if frappe.db.has_column("Item", "gender") else "null"

	return f"""
		select i.name as item_code, i.item_name, i.brand, i.item_group, {gender} as gender,
			i.image, i.disabled, i.has_variants, i.creation,
			ifnull(st.stock, 0) as stock,
			if(i.has_variants, ifnull(vs.variants, 0), 1) as variants,
			if(i.has_variants, ifnull(vs.linked_variants, 0), if(lk.name is null, 0, 1)) as linked_variants,
			if(i.has_variants, ifnull(vs.unpriced, 0), if(i.disabled = 0 and {rate_i} <= 0, 1, 0)) as unpriced,
			if(i.has_variants, ifnull(vs.unlinked_in_stock, 0), 0) as unlinked_in_stock,
			if(i.has_variants, vs.min_rate, nullif({rate_i}, 0)) as min_rate,
			if(i.has_variants, vs.max_rate, nullif({rate_i}, 0)) as max_rate,
			lk.integration_item_code as product_id,
			sp.status as shopify_status, ifnull(sp.published, 0) as published,
			sp.image as shopify_image, sp.title as shopify_title,
			ifnull(rt.pending, 0) as pending
		from tabItem i
		left join (
			select if(ifnull(it.variant_of, '') = '', it.name, it.variant_of) as product,
				sum(b.actual_qty) as stock
			from tabBin b join tabItem it on it.name = b.item_code
			where b.warehouse in %(warehouses)s
			group by 1
		) st on st.product = i.name
		left join (
			select v.variant_of as product,
				sum(v.disabled = 0) as variants,
				sum(v.disabled = 0 and e.name is not null) as linked_variants,
				sum(v.disabled = 0 and {rate_v} <= 0) as unpriced,
				sum(v.disabled = 0 and e.name is null and ifnull(vb.stock, 0) > 0) as unlinked_in_stock,
				min(if(v.disabled = 0, nullif({rate_v}, 0), null)) as min_rate,
				max(if(v.disabled = 0, nullif({rate_v}, 0), null)) as max_rate
			from tabItem v
			left join `tabEcommerce Item` e
				on e.erpnext_item_code = v.name and e.integration = '{MODULE_NAME}'
			left join (
				select item_code, sum(actual_qty) as stock from tabBin
				where warehouse in %(warehouses)s group by item_code
			) vb on vb.item_code = v.name
			where ifnull(v.variant_of, '') <> ''
			group by v.variant_of
		) vs on vs.product = i.name
		left join `tabEcommerce Item` lk
			on lk.erpnext_item_code = i.name and lk.integration = '{MODULE_NAME}'
		left join `tab{SNAPSHOT}` sp on sp.name = lk.integration_item_code
		left join (
			select if(ifnull(it.variant_of, '') = '', it.name, it.variant_of) as product, count(*) as pending
			from `tabEcommerce Integration Log` l
			join tabItem it on it.name = json_unquote(json_extract(l.request_data, '$.item_code'))
			where l.method = %(retry_method)s and l.status = 'Error'
			group by 1
		) rt on rt.product = i.name
		where ifnull(i.variant_of, '') = ''
	"""


def _setting():
	return frappe.get_cached_doc(SETTING_DOCTYPE)


def _mapping() -> dict:
	"""{ERPNext warehouse: Shopify location name or id}, in mapping order."""
	setting = _setting()
	return {
		row.erpnext_warehouse: (row.shopify_location_name or row.shopify_location_id)
		for row in setting.shopify_warehouse_mapping
	}


def _context(filters: dict):
	"""SQL pieces shared by counts, rows and export."""
	mapping = _mapping()
	store = filters.get("store")
	warehouses = [store] if store in mapping else list(mapping)
	values = {"warehouses": tuple(warehouses) or ("",), "retry_method": RETRY_METHOD}

	conds = []
	if filters.get("search"):
		values["search"] = f"%{cstr(filters['search']).strip()}%"
		conds.append(
			"(p.item_code like %(search)s or p.item_name like %(search)s"
			" or exists (select 1 from tabItem sv where sv.variant_of = p.item_code and sv.name like %(search)s))"
		)
	for key in ("brand", "item_group", "gender"):
		if filters.get(key):
			values[key] = filters[key]
			conds.append(f"p.{key} = %({key})s")

	where = " and ".join(conds) or "1 = 1"
	return f"({_product_sql()}) p", where, values


def _only_shopify_where(filters: dict, values: dict) -> str:
	where = [
		f"""sp.name not in (select integration_item_code from `tabEcommerce Item`
			where integration = '{MODULE_NAME}' and ifnull(integration_item_code, '') <> '')"""
	]
	if filters.get("search"):
		values["search"] = f"%{cstr(filters['search']).strip()}%"
		where.append("(sp.title like %(search)s or sp.skus like %(search)s or sp.name like %(search)s)")
	return " and ".join(where)


@frappe.whitelist()
def get_catalogue(bucket="not_in_shopify", filters=None, start=0, sort="stock"):
	_check_access()
	filters = frappe.parse_json(filters) or {}
	start = max(cint(start), 0)
	base, where, values = _context(filters)

	counts = frappe.db.sql(
		f"select {', '.join(f'sum({cond}) as `{key}`' for key, cond in BUCKETS.items())} from {base} where {where}",
		values,
		as_dict=1,
	)[0]
	counts = {key: cint(value) for key, value in counts.items()}

	only_values = {}
	counts["only_shopify"] = cint(
		frappe.db.sql(
			f"select count(*) from `tab{SNAPSHOT}` sp where {_only_shopify_where(filters, only_values)}",
			only_values,
		)[0][0]
	)

	if bucket == "only_shopify":
		rows = frappe.db.sql(
			f"""select sp.name as product_id, sp.title as item_name, sp.status as shopify_status,
				sp.published, sp.image as shopify_image, sp.variants_count as variants,
				sp.total_inventory as shopify_inventory, sp.vendor as brand, sp.product_type as item_group,
				sp.skus
			from `tab{SNAPSHOT}` sp where {_only_shopify_where(filters, only_values)}
			order by sp.title limit %(start)s, {PAGE_LENGTH}""",
			dict(only_values, start=start),
			as_dict=1,
		)
	else:
		bucket = bucket if bucket in BUCKETS else "all"
		order = SORTS.get(sort, SORTS["stock"])
		rows = frappe.db.sql(
			f"select p.* from {base} where {where} and {BUCKETS[bucket]} order by {order}"
			f" limit %(start)s, {PAGE_LENGTH}",
			dict(values, start=start),
			as_dict=1,
		)
		_add_store_stock(rows)

	return {
		"rows": rows,
		"total": counts.get(bucket, 0),
		"counts": counts,
		"start": start,
		"page_length": PAGE_LENGTH,
	}


def _add_store_stock(rows) -> None:
	"""Per-store stock for the visible products (shown on hover / in the panel)."""
	codes = [r.item_code for r in rows if r.item_code]
	mapping = _mapping()
	if not codes or not mapping:
		return
	data = frappe.db.sql(
		"""select if(ifnull(it.variant_of, '') = '', it.name, it.variant_of) as product,
			b.warehouse, sum(b.actual_qty) as qty
		from tabBin b join tabItem it on it.name = b.item_code
		where b.warehouse in %(warehouses)s and (it.name in %(codes)s or it.variant_of in %(codes)s)
		group by 1, 2""",
		{"warehouses": tuple(mapping), "codes": tuple(codes)},
		as_dict=1,
	)
	per_product = {}
	for d in data:
		per_product.setdefault(d.product, {})[d.warehouse] = flt(d.qty)
	for row in rows:
		stores = per_product.get(row.item_code, {})
		row["stores"] = [{"store": mapping[w], "qty": stores.get(w, 0)} for w in mapping]


@frappe.whitelist()
def get_meta():
	"""Filter options and page facts, read once when the page opens."""
	_check_access()
	setting = _setting()
	cache = frappe.cache()

	def distinct(column):
		if not frappe.db.has_column("Item", column):
			return []
		return frappe.db.sql_list(
			f"""select distinct `{column}` from tabItem
			where ifnull(variant_of, '') = '' and ifnull(`{column}`, '') <> '' order by 1"""
		)

	options = cache.get_value("shopify_catalogue:options")
	if not options:
		options = {"brands": distinct("brand"), "item_groups": distinct("item_group"), "genders": distinct("gender")}
		cache.set_value("shopify_catalogue:options", options, expires_in_sec=600)

	shop = cstr(setting.shopify_url).replace("https://", "").replace("http://", "").strip("/")
	return dict(
		options,
		stores=[{"warehouse": w, "name": n} for w, n in _mapping().items()],
		shop=shop,
		shop_handle=shop.replace(".myshopify.com", ""),
		enabled=bool(setting.is_enabled()),
		new_products_active=bool(setting.sync_new_item_as_active),
		snapshot=_snapshot_state(),
		job=_get_job(),
	)


def _snapshot_state() -> dict:
	refreshed = frappe.cache().get_value(REFRESHED_KEY)
	if not refreshed:
		refreshed = frappe.db.sql(f"select max(modified) from `tab{SNAPSHOT}`")[0][0]
	return {
		"refreshed_at": cstr(refreshed) if refreshed else None,
		"products": frappe.db.count(SNAPSHOT),
		"refreshing": _is_queued(REFRESH_JOB_NAME),
	}


@frappe.whitelist()
def get_product_detail(item_code):
	_check_access()
	item = frappe.db.get_value(
		"Item",
		item_code,
		["name", "item_name", "item_group", "brand", "image", "disabled", "has_variants", "description"],
		as_dict=1,
	)
	if not item:
		frappe.throw(_("Item {0} not found").format(item_code))

	mapping = _mapping()
	rate = _rate_column()
	names = (
		frappe.get_all("Item", filters={"variant_of": item_code}, pluck="name", order_by="name")
		if item.has_variants
		else [item.name]
	)
	variants = []
	if names:
		info = {
			d.name: d
			for d in frappe.get_all(
				"Item",
				filters={"name": ["in", names]},
				fields=["name", "item_name", "disabled"] + ([rate] if rate else []),
			)
		}
		attrs = {}
		for a in frappe.get_all(
			"Item Variant Attribute",
			filters={"parent": ["in", names]},
			fields=["parent", "attribute_value"],
			order_by="idx asc",
		):
			attrs.setdefault(a.parent, []).append(cstr(a.attribute_value))
		links = {
			e.erpnext_item_code: e
			for e in frappe.get_all(
				"Ecommerce Item",
				filters={"integration": MODULE_NAME, "erpnext_item_code": ["in", names]},
				fields=["erpnext_item_code", "variant_id", "integration_item_code"],
			)
		}
		stock = {}
		if mapping:
			for b in frappe.get_all(
				"Bin",
				filters={"item_code": ["in", names], "warehouse": ["in", list(mapping)]},
				fields=["item_code", "warehouse", "actual_qty"],
			):
				stock.setdefault(b.item_code, {})[b.warehouse] = flt(b.actual_qty)
		pending = {
			json.loads(l.request_data or "{}").get("item_code"): l.message
			for l in frappe.get_all(
				"Ecommerce Integration Log",
				filters={"method": RETRY_METHOD, "status": "Error"},
				fields=["request_data", "message"],
			)
		}
		for name in names:
			d = info.get(name) or frappe._dict(name=name)
			variants.append(
				{
					"item_code": name,
					"label": " / ".join(attrs.get(name, [])) or d.get("item_name") or name,
					"disabled": cint(d.get("disabled")),
					"price": flt(d.get(rate)) if rate else 0,
					"stock": sum(stock.get(name, {}).values()),
					"stores": [{"store": mapping[w], "qty": stock.get(name, {}).get(w, 0)} for w in mapping],
					"in_shopify": name in links,
					"pending": pending.get(name),
				}
			)

	product_id = frappe.db.get_value(
		"Ecommerce Item", {"erpnext_item_code": item_code, "integration": MODULE_NAME}, "integration_item_code"
	)
	snapshot = frappe.db.get_value(SNAPSHOT, product_id, "*", as_dict=1) if product_id else None
	return {"item": item, "variants": variants, "product_id": product_id, "shopify": snapshot}


# ---------------------------------------------------------------------------
# Shopify copy (snapshot)
# ---------------------------------------------------------------------------

SNAPSHOT_COLUMNS = (
	"product_id",
	"title",
	"status",
	"published",
	"handle",
	"product_type",
	"vendor",
	"variants_count",
	"total_inventory",
	"shopify_updated_at",
	"skus",
	"tags",
	"image",
)


def _snapshot_row(d: dict) -> dict:
	variants = d.get("variants") or []
	updated = cstr(d.get("updated_at"))[:19].replace("T", " ")
	return {
		"product_id": cstr(d.get("id")),
		"title": cstr(d.get("title"))[:140],
		"status": cstr(d.get("status")),
		"published": 1 if d.get("published_at") else 0,
		"handle": cstr(d.get("handle"))[:140],
		"product_type": cstr(d.get("product_type"))[:140],
		"vendor": cstr(d.get("vendor"))[:140],
		"variants_count": len(variants),
		"total_inventory": sum(cint(v.get("inventory_quantity")) for v in variants),
		"shopify_updated_at": get_datetime(updated) if updated else None,
		"skus": ", ".join(cstr(v.get("sku")) for v in variants if v.get("sku"))[:5000],
		"tags": cstr(d.get("tags"))[:2000],
		"image": cstr((d.get("image") or {}).get("src")) or None,
	}


def _upsert_snapshot(rows) -> None:
	if not rows:
		return
	stamp = now()
	columns = ("name", "creation", "modified", "owner", "modified_by", "docstatus", "idx") + SNAPSHOT_COLUMNS
	placeholders = ", ".join(["%s"] * len(columns))
	updates = ", ".join(f"`{c}` = values(`{c}`)" for c in ("modified",) + SNAPSHOT_COLUMNS)
	for row in rows:
		values = [row["product_id"], stamp, stamp, "Administrator", "Administrator", 0, 0]
		values += [row[c] for c in SNAPSHOT_COLUMNS]
		frappe.db.sql(
			f"""insert into `tab{SNAPSHOT}` ({', '.join(f'`{c}`' for c in columns)})
			values ({placeholders}) on duplicate key update {updates}""",
			values,
		)


def remember_product(product) -> None:
	"""Keep the copy current after an upload. Never fails the caller."""
	try:
		data = product.to_dict() if hasattr(product, "to_dict") else dict(product)
		if data.get("id"):
			_upsert_snapshot([_snapshot_row(data)])
	except Exception:
		frappe.log_error(title="Shopify Catalogue: snapshot update failed")


def refresh_shopify_snapshot() -> None:
	"""Hourly (and on demand): copy every Shopify product's status/publication."""
	setting = frappe.get_doc(SETTING_DOCTYPE)
	if not setting.is_enabled():
		return
	seen = _fetch_snapshot()
	if seen is None:
		return
	# products deleted in Shopify
	existing = frappe.get_all(SNAPSHOT, pluck="name")
	gone = [name for name in existing if name not in seen]
	for i in range(0, len(gone), 500):
		frappe.db.delete(SNAPSHOT, {"name": ["in", gone[i : i + 500]]})
	frappe.cache().set_value(REFRESHED_KEY, now())
	frappe.db.commit()


@temp_shopify_session
def _fetch_snapshot():
	seen = set()
	fields = "id,title,handle,status,published_at,product_type,vendor,updated_at,variants,image,tags"
	for page in PaginatedIterator(Product.find(limit=250, fields=fields)):
		rows = [_snapshot_row(p.to_dict()) for p in page]
		_upsert_snapshot(rows)
		frappe.db.commit()
		seen.update(r["product_id"] for r in rows)
		time.sleep(0.5)
	return seen


@frappe.whitelist()
def start_refresh():
	_check_access()
	if not _is_queued(REFRESH_JOB_NAME):
		frappe.enqueue(
			"ecommerce_integrations.shopify.catalogue.refresh_shopify_snapshot",
			queue="long",
			timeout=1800,
			job_name=REFRESH_JOB_NAME,
		)
	return _snapshot_state()


@frappe.whitelist()
def get_status():
	_check_access()
	return {"job": _get_job(), "snapshot": _snapshot_state()}


def _is_queued(job_name: str) -> bool:
	from frappe.utils.background_jobs import get_jobs

	site = frappe.local.site
	return job_name in get_jobs(site=site, queue="long", key="job_name").get(site, [])


# ---------------------------------------------------------------------------
# Actions (background jobs, one at a time)
# ---------------------------------------------------------------------------


def _get_job():
	job = frappe.cache().get_value(JOB_KEY)
	if job and job.get("status") == "running" and not _is_queued(JOB_NAME):
		# the worker died or was restarted mid-run
		job["status"] = "stopped"
		frappe.cache().set_value(JOB_KEY, job)
	return job


def _resolve_targets(item_codes, bucket, filters, key="item_code"):
	if item_codes:
		targets = frappe.parse_json(item_codes)
		return [cstr(t) for t in targets if t][:BULK_LIMIT]

	filters = frappe.parse_json(filters) or {}
	if bucket == "only_shopify":
		values = {}
		return frappe.db.sql_list(
			f"select sp.name from `tab{SNAPSHOT}` sp where {_only_shopify_where(filters, values)}"
			f" order by sp.title limit {BULK_LIMIT}",
			values,
		)
	base, where, values = _context(filters)
	cond = BUCKETS.get(bucket, BUCKETS["all"])
	column = "p.product_id" if key == "product_id" else "p.item_code"
	return [
		cstr(v)
		for v in frappe.db.sql_list(
			f"select {column} from {base} where {where} and {cond} and {column} is not null"
			f" order by {SORTS['stock']} limit {BULK_LIMIT}",
			values,
		)
	]


def _start_job(kind: str, targets: list):
	job = _get_job()
	if job and job.get("status") == "running":
		frappe.throw(_("Another Shopify Catalogue task is still running."))
	if not targets:
		frappe.throw(_("Nothing to do: no products match."))

	job = {
		"kind": kind,
		"status": "running",
		"total": len(targets),
		"done": 0,
		"ok": 0,
		"skipped": 0,
		"failed": 0,
		"current": None,
		"problems": [],
		"started_at": now(),
		"finished_at": None,
		"user": frappe.session.user,
		"stop": False,
	}
	frappe.cache().set_value(JOB_KEY, job)
	frappe.enqueue(
		"ecommerce_integrations.shopify.catalogue.run_job",
		queue="long",
		timeout=6 * 3600,
		job_name=JOB_NAME,
		kind=kind,
		targets=targets,
	)
	return job


@frappe.whitelist()
def send_to_shopify(item_codes=None, bucket=None, filters=None):
	_check_access()
	return _start_job("send", _resolve_targets(item_codes, bucket, filters))


@frappe.whitelist()
def publish(product_ids=None, bucket=None, filters=None):
	_check_access()
	return _start_job("publish", _resolve_targets(product_ids, bucket, filters, key="product_id"))


@frappe.whitelist()
def import_from_shopify(product_ids=None, bucket=None, filters=None):
	_check_access()
	return _start_job("import", _resolve_targets(product_ids, "only_shopify", filters))


@frappe.whitelist()
def stop_job():
	_check_access()
	job = _get_job()
	if job and job.get("status") == "running":
		job["stop"] = True
		frappe.cache().set_value(JOB_KEY, job)
	return job


def run_job(kind: str, targets: list):
	from ecommerce_integrations.shopify.product import _is_database_error, _is_shop_wide_error, _short_error

	handler = {"send": _send_product, "publish": _publish_product, "import": _import_product}[kind]
	cache = frappe.cache()
	frappe.flags.in_shopify_item_upload = True  # outages are reported here, not logged per call

	for target in targets:
		job = cache.get_value(JOB_KEY) or {}
		if job.get("stop"):
			job["status"] = "stopped"
			break
		job["current"] = target
		cache.set_value(JOB_KEY, job)

		outcome, note = "ok", None
		try:
			note = handler(target)
		except SkipProduct as e:
			outcome, note = "skipped", cstr(e)
		except Exception as e:
			if _is_database_error(e):
				frappe.db.rollback()
			frappe.clear_last_message()
			outcome, note = "failed", _short_error(e)
			if _is_shop_wide_error(e):
				job = cache.get_value(JOB_KEY) or job
				_record(job, target, outcome, note)
				job["status"] = "stopped"
				job["stopped_reason"] = _("Shopify is not answering ({0}). Try again later.").format(note)
				break
		frappe.db.commit()

		job = cache.get_value(JOB_KEY) or job
		_record(job, target, outcome, note)
		cache.set_value(JOB_KEY, job)
		time.sleep(0.5)  # Shopify REST allows about 2 calls per second
	else:
		job = cache.get_value(JOB_KEY) or {}
		job["status"] = "finished"

	frappe.flags.in_shopify_item_upload = False
	job["current"] = None
	job["finished_at"] = now()
	cache.set_value(JOB_KEY, job)
	frappe.publish_realtime("shopify_catalogue_job", job, user=job.get("user"))


def _record(job: dict, target: str, outcome: str, note) -> None:
	job["done"] = cint(job.get("done")) + 1
	job[outcome] = cint(job.get(outcome)) + 1
	if outcome != "ok":
		problems = job.setdefault("problems", [])
		problems.append({"target": target, "outcome": outcome, "note": note})
		del problems[:-200]


def _send_product(item_code: str) -> str:
	from ecommerce_integrations.shopify import product as sp

	doc = frappe.get_doc("Item", item_code)
	if doc.disabled:
		raise SkipProduct(_("Disabled in ERPNext"))

	rate = _rate_column()
	if not doc.has_variants:
		if sp._is_linked(doc.name):
			raise SkipProduct(_("Already in Shopify"))
		if not rate or flt(doc.get(rate)) <= 0:
			raise SkipProduct(_("No Shopify price"))
		sp._upload_erpnext_item(doc)
		return _("Created")

	variants = frappe.get_all(
		"Item",
		filters={"variant_of": doc.name, "disabled": 0},
		fields=["name"] + ([rate] if rate else []),
		order_by="name",
	)
	priced = [v.name for v in variants if rate and flt(v.get(rate)) > 0]
	unpriced = len(variants) - len(priced)
	if not priced:
		raise SkipProduct(_("No variant has a Shopify price"))

	if not sp._is_linked(doc.name):
		_create_product_with_variants(doc, priced)
		note = _("Created with {0} variants").format(len(priced))
	else:
		missing = [name for name in priced if not sp._is_linked(name)]
		if not missing:
			raise SkipProduct(_("Already in Shopify"))
		for name in missing:
			sp._upload_erpnext_item(frappe.get_doc("Item", name), sync_collections=False)
			frappe.db.commit()
		note = _("Added {0} variants").format(len(missing))

	if unpriced:
		note += " · " + _("{0} without a Shopify price left out").format(unpriced)
	return note


@temp_shopify_session
def _create_product_with_variants(template, variant_names):
	"""Create a new product with all its variants in one call (instead of one
	upload per variant), then link everything in ERPNext."""
	from ecommerce_integrations.shopify import product as sp

	if len(variant_names) > MAX_VARIANTS:
		raise SkipProduct(_("{0} variants: Shopify allows {1} per product").format(len(variant_names), MAX_VARIANTS))
	if len(template.attributes) > 3:
		raise SkipProduct(_("More than 3 attributes: Shopify allows 3 options"))

	setting = frappe.get_doc(SETTING_DOCTYPE)
	product = Product()
	product.published = False
	product.status = "active" if setting.sync_new_item_as_active else "draft"
	sp.map_erpnext_item_to_shopify(shopify_product=product, erpnext_item=template)

	docs = [frappe.get_doc("Item", name) for name in variant_names]
	variants = []
	for d in docs:
		attributes = sp._variant_attributes(d, template)
		if template.is_stock_item:
			attributes["inventory_management"] = "shopify"
		variants.append(attributes)

	# options carry exactly the values the sent variants use, in attribute order
	options = []
	for i, option in enumerate(sp._product_options(template, {})):
		used = {v.get(f"option{i + 1}") for v in variants} - {None, ""}
		values = [value for value in option["values"] if value in used]
		values += sorted(used - set(values), key=cstr)
		options.append({"name": option["name"], "values": values})

	product.options = options
	product.variants = [Variant(v) for v in variants]
	sp._save_product(product)

	sp._insert_ecommerce_item(template, product)
	by_sku = {cstr(v.sku): v for v in product.variants}
	for d in docs:
		variant = by_sku.get(d.item_code)
		if variant:
			sp._insert_ecommerce_item(d, product, variant)

	sp.sync_collections_from_erp_item(product.id, template)
	sp.write_upload_log(status=True, product=product, item=template)


@temp_shopify_session
def _publish_product(product_id: str) -> str:
	from ecommerce_integrations.shopify import product as sp

	item_code = frappe.db.get_value(
		"Ecommerce Item",
		{"integration": MODULE_NAME, "integration_item_code": product_id, "variant_of": ["is", "not set"]},
		"erpnext_item_code",
	)
	if item_code and frappe.db.get_value("Item", item_code, "disabled"):
		raise SkipProduct(_("Disabled in ERPNext"))

	product = Product.find(product_id)
	if product.status == "active" and product.published_at:
		remember_product(product)
		raise SkipProduct(_("Already active and published"))

	product.status = "active"
	product.published = True
	tags = [t.strip() for t in cstr(getattr(product, "tags", "")).split(",") if t.strip()]
	if sp.ERP_DISABLED_TAG in tags:
		product.tags = ", ".join(t for t in tags if t != sp.ERP_DISABLED_TAG)
	sp._save_product(product)
	remember_product(product)
	return _("Active and published")


def _import_product(product_id: str) -> str:
	from ecommerce_integrations.shopify.page.shopify_import_products.shopify_import_products import (
		_resync_product,
	)

	if not _resync_product(product_id):
		raise Exception(_("Shopify product {0} could not be imported").format(product_id))
	return _("Imported")


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------


@frappe.whitelist()
def export_catalogue(bucket="all", filters=None):
	_check_access()
	filters = frappe.parse_json(filters) or {}
	out = io.StringIO()
	writer = csv.writer(out)

	if bucket == "only_shopify":
		values = {}
		rows = frappe.db.sql(
			f"""select sp.name, sp.title, sp.status, sp.published, sp.vendor, sp.product_type,
				sp.variants_count, sp.total_inventory, sp.skus
			from `tab{SNAPSHOT}` sp where {_only_shopify_where(filters, values)} order by sp.title
			limit {EXPORT_LIMIT}""",
			values,
		)
		writer.writerow(
			["Shopify Product ID", "Title", "Status", "Published", "Vendor", "Type", "Variants", "Shopify Inventory", "SKUs"]
		)
		writer.writerows(rows)
	else:
		base, where, values = _context(filters)
		cond = BUCKETS.get(bucket, BUCKETS["all"])
		rows = frappe.db.sql(
			f"""select p.item_code, p.item_name, p.brand, p.item_group, p.gender, p.disabled, p.stock,
				p.variants, p.linked_variants, p.unpriced, p.min_rate, p.max_rate,
				p.product_id, p.shopify_status, p.published, p.pending
			from {base} where {where} and {cond} order by {SORTS['stock']} limit {EXPORT_LIMIT}""",
			values,
		)
		writer.writerow(
			[
				"Item Code",
				"Item Name",
				"Brand",
				"Item Group",
				"Gender",
				"Disabled",
				"Stock (Shopify stores)",
				"Variants",
				"Variants in Shopify",
				"Variants without Shopify price",
				"Lowest Shopify price",
				"Highest Shopify price",
				"Shopify Product ID",
				"Shopify Status",
				"Published",
				"Waiting to sync",
			]
		)
		writer.writerows(rows)

	frappe.response["type"] = "csv"
	frappe.response["doctype"] = f"shopify-catalogue-{bucket}"
	frappe.response["result"] = out.getvalue()


@frappe.whitelist()
def get_asset_version() -> str:
	"""Version of the page's script and stylesheet (newest file time), so the
	loader always fetches the current code. See shopify_catalogue.js."""
	import os

	public = frappe.get_app_path("ecommerce_integrations", "public")
	times = [
		os.path.getmtime(os.path.join(public, path))
		for path in ("js/shopify_catalogue.js", "css/shopify_catalogue.css")
		if os.path.exists(os.path.join(public, path))
	]
	return str(int(max(times))) if times else "0"
