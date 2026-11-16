"""Shopify price from a price list.

Shopify price = the item's price in the chosen price list that is valid today
(own price, else its template's), plus the item's tax for Shopify Setting's
tax category (its Item Tax Template; the setting's fallback % when it has
none), rounded to a whole unit. It is kept in Item.shopify_selling_rate and
sent to Shopify whenever it changes:

- every Item Price save, import or delete queues the item (and, for a
  template's price, its variants); one background job drains the queue;
- a daily job picks up prices whose "valid from" / "valid upto" date arrives;
- an Item save fills the field before the item is uploaded;
- the Shopify Catalogue page can preview and apply it in bulk.

Replaces the site scripts "Item Price-Form" (client) + "Update Shopping
Selling Rate" (server), which only ran when a price was saved in the form.
"""

from decimal import ROUND_HALF_UP, Decimal

import frappe
from frappe import _
from frappe.utils import add_days, cint, flt, getdate, nowdate

from ecommerce_integrations.shopify.constants import ITEM_SELLING_RATE_FIELD, MODULE_NAME, SETTING_DOCTYPE

PENDING_KEY = "shopify_price:pending"
JOB_NAME = "shopify.price.sync"
CHUNK = 500


def _setting():
	return frappe.get_cached_doc(SETTING_DOCTYPE)


def is_enabled(setting=None) -> bool:
	setting = setting or _setting()
	return bool(
		cint(setting.get("sync_price_from_price_list")) and setting.get("shopify_price_list")
	) and frappe.db.has_column("Item", ITEM_SELLING_RATE_FIELD)


def whole(value) -> float:
	"""Round half up to a whole unit (Python's round() would round half to even)."""
	return float(Decimal(str(flt(value, 6))).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


# ---------------------------------------------------------------------------
# Calculation (set-based: one query per kind of data, for any number of items)
# ---------------------------------------------------------------------------


def compute_prices(item_codes, setting=None) -> dict:
	"""{item_code: {"base", "tax_rate", "new", "old"}}; new is None when the
	price list has no price valid today for the item (nor its template)."""
	setting = setting or _setting()
	out = {}
	codes = list(dict.fromkeys(c for c in item_codes if c))
	for i in range(0, len(codes), CHUNK):
		out.update(_compute_chunk(codes[i : i + CHUNK], setting))
	return out


def _valid_prices(codes, setting, stock_uom=None) -> dict:
	"""{item_code: rate} — newest price valid today (stock UOM / no UOM first)."""
	if not codes:
		return {}
	stock_uom = stock_uom or {}
	today = getdate(nowdate())
	prices = {}
	for p in frappe.db.sql(
		"""select item_code, price_list_rate, uom from `tabItem Price`
		where price_list = %(pl)s and selling = 1 and ifnull(customer, '') = ''
			and item_code in %(codes)s
			and (valid_from is null or valid_from <= %(today)s)
			and (valid_upto is null or valid_upto >= %(today)s)
		order by ifnull(valid_from, '1900-01-01') desc, modified desc""",
		{"pl": setting.shopify_price_list, "codes": tuple(codes), "today": today},
		as_dict=1,
	):
		uom_ok = not p.uom or p.item_code not in stock_uom or p.uom == stock_uom[p.item_code]
		current = prices.get(p.item_code)
		if current is None or (uom_ok and not current[1]):
			prices[p.item_code] = (flt(p.price_list_rate), uom_ok)
	return {code: rate for code, (rate, _ok) in prices.items()}


def _template_rates() -> dict:
	return {
		r[0]: flt(r[1])
		for r in frappe.db.sql("select parent, sum(tax_rate) from `tabItem Tax Template Detail` group by parent")
	}


def _tax_rates(codes, setting) -> dict:
	"""{item_code: tax %} from the newest Item Tax row valid today for the
	Shopify tax category. Items without one are absent."""
	if not codes or not setting.get("tax_category"):
		return {}
	rates = _template_rates()
	taxes = {}
	for t in frappe.db.sql(
		"""select parent, item_tax_template from `tabItem Tax`
		where parenttype = 'Item' and parent in %(codes)s and tax_category = %(cat)s
			and ifnull(item_tax_template, '') <> ''
			and (valid_from is null or valid_from <= %(today)s)
		order by ifnull(valid_from, '1900-01-01') desc""",
		{"codes": tuple(codes), "cat": setting.tax_category, "today": getdate(nowdate())},
		as_dict=1,
	):
		taxes.setdefault(t.parent, rates.get(t.item_tax_template, 0.0))
	return taxes


def _compute_chunk(codes, setting) -> dict:
	items = {
		d.name: d
		for d in frappe.get_all(
			"Item",
			filters={"name": ["in", codes]},
			fields=["name", "variant_of", "stock_uom", "has_variants", ITEM_SELLING_RATE_FIELD],
		)
	}
	lookup = list({*items, *(d.variant_of for d in items.values() if d.variant_of)})
	prices = _valid_prices(lookup, setting, {d.name: d.stock_uom for d in items.values()})
	taxes = _tax_rates(lookup, setting)
	fallback = flt(setting.get("price_tax_rate"))

	out = {}
	for code in codes:
		item = items.get(code)
		if not item or item.has_variants:
			continue
		base = prices.get(code, prices.get(item.variant_of))
		tax_rate = taxes.get(code, taxes.get(item.variant_of, fallback))
		new = whole(base * (1 + tax_rate / 100)) if base is not None else None
		out[code] = {"base": base, "tax_rate": tax_rate, "new": new, "old": flt(item.get(ITEM_SELLING_RATE_FIELD))}
	return out


def sellable_items(product_codes) -> list:
	"""Simple items and enabled variants behind these products (templates)."""
	codes = list(dict.fromkeys(c for c in product_codes if c))
	result = []
	for i in range(0, len(codes), CHUNK):
		chunk = codes[i : i + CHUNK]
		result += frappe.db.sql_list(
			"""select name from tabItem
			where (name in %(c)s and has_variants = 0) or (variant_of in %(c)s and disabled = 0)""",
			{"c": tuple(chunk)},
		)
	return result


# ---------------------------------------------------------------------------
# Applying
# ---------------------------------------------------------------------------


def apply_prices(item_codes, setting=None) -> dict:
	"""Store changed prices and send them to Shopify. Returns counts."""
	from ecommerce_integrations.shopify.product import _is_shop_wide_error, _queue_item_retry

	setting = setting or _setting()
	stats = {"changed": 0, "unchanged": 0, "no_price": 0, "sent": 0, "failed": 0}
	prices = compute_prices(item_codes, setting)
	variants = _variant_ids([code for code, p in prices.items() if p["new"] is not None and p["new"] != p["old"]])

	for code, p in prices.items():
		if p["new"] is None:
			stats["no_price"] += 1
			continue
		if p["new"] == p["old"]:
			stats["unchanged"] += 1
			continue
		frappe.db.set_value("Item", code, ITEM_SELLING_RATE_FIELD, p["new"], update_modified=False)
		stats["changed"] += 1
		variant_id = variants.get(code)
		if variant_id and setting.is_enabled():
			try:
				_push_price(variant_id, p["new"])
				stats["sent"] += 1
			except Exception as e:
				if _is_shop_wide_error(e):
					raise
				stats["failed"] += 1
				_queue_item_retry(code, e)  # the 30-minute retry sends the whole item
		frappe.db.commit()
	return stats


def _variant_ids(item_codes) -> dict:
	if not item_codes:
		return {}
	return {
		e.erpnext_item_code: e.variant_id
		for e in frappe.get_all(
			"Ecommerce Item",
			filters={"integration": MODULE_NAME, "erpnext_item_code": ["in", item_codes], "variant_id": ["is", "set"]},
			fields=["erpnext_item_code", "variant_id"],
		)
	}


def _push_price(variant_id, price) -> None:
	"""One call: PUT /variants/{id}.json with the new price."""
	from shopify.resources import Variant

	from ecommerce_integrations.shopify.connection import temp_shopify_session
	from ecommerce_integrations.shopify.inventory import _call_shopify

	@temp_shopify_session
	def push():
		# an empty product prefix makes the library use Shopify's documented
		# PUT /variants/{id}.json instead of /products/{product}/variants/...
		variant = Variant({"id": cint(variant_id), "price": price}, prefix_options={"product_id": None})
		if not _call_shopify(variant.save):
			raise Exception(", ".join(variant.errors.full_messages()) or _("Shopify refused the price"))

	push()


# ---------------------------------------------------------------------------
# Automatic: hooks, queue, daily job
# ---------------------------------------------------------------------------


def on_item_price_change(doc, method=None):
	"""Item Price on_update / on_trash: queue the item(s) whose price moved."""
	setting = _setting()
	if not is_enabled(setting):
		return
	codes = set()
	before = doc.get_doc_before_save() if method == "on_update" else None
	for d in (doc, before):
		if d and d.price_list == setting.shopify_price_list and cint(d.selling):
			codes.add(d.item_code)
	if codes:
		queue(codes)


def queue(item_codes) -> None:
	"""Remember items to re-price and start the job once the save commits."""
	from ecommerce_integrations.shopify.catalogue import _is_queued

	item_codes = [c for c in item_codes if c]
	if item_codes:
		frappe.cache().sadd(PENDING_KEY, *item_codes)
	if not _is_queued(JOB_NAME):
		frappe.enqueue(
			"ecommerce_integrations.shopify.pricing.process_queue",
			queue="long",
			timeout=3600,
			job_name=JOB_NAME,
			enqueue_after_commit=True,
		)


def process_queue() -> None:
	setting = _setting()
	if not is_enabled(setting):
		return
	frappe.set_user("Administrator")
	cache = frappe.cache()
	while True:
		codes = []
		for _i in range(CHUNK):
			code = cache.spop(PENDING_KEY)
			if not code:
				break
			codes.append(frappe.safe_decode(code))
		if not codes:
			break
		# a template's price is its variants' price when they have none of their own
		apply_prices(sellable_items(codes), setting)


def on_item_validate(doc, method=None):
	"""Item save: carry the current price-list price, so an upload that follows
	(new item or variant, any change) sends the right price. The tax comes from
	the rows being saved, so changing the item's tax template re-prices it."""
	if doc.has_variants or not is_enabled():
		return
	setting = _setting()
	codes = [c for c in (None if doc.is_new() else doc.name, doc.variant_of) if c]
	prices = _valid_prices(codes, setting, {doc.name: doc.stock_uom})
	base = prices.get(doc.name, prices.get(doc.variant_of))
	if base is None:
		return

	today = getdate(nowdate())
	rows = sorted(
		(
			r
			for r in doc.get("taxes") or []
			if r.tax_category == setting.get("tax_category") and r.item_tax_template
			and (not r.valid_from or getdate(r.valid_from) <= today)
		),
		key=lambda r: getdate(r.valid_from or "1900-01-01"),
		reverse=True,
	)
	if rows:
		tax_rate = _template_rates().get(rows[0].item_tax_template, 0.0)
	else:
		tax_rate = _tax_rates([doc.variant_of], setting).get(doc.variant_of, flt(setting.get("price_tax_rate")))
	doc.set(ITEM_SELLING_RATE_FIELD, whole(base * (1 + tax_rate / 100)))


def queue_prices_changing_today() -> None:
	"""Daily: prices that start today or ended yesterday change the price."""
	setting = _setting()
	if not is_enabled(setting):
		return
	today = nowdate()
	codes = frappe.db.sql_list(
		"""select distinct item_code from `tabItem Price`
		where price_list = %s and selling = 1 and (valid_from = %s or valid_upto = %s)""",
		(setting.shopify_price_list, today, add_days(today, -1)),
	)
	if codes or frappe.cache().smembers(PENDING_KEY):
		queue(codes)
