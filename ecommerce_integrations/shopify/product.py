import json
import socket
import time
from typing import Optional, List

import frappe
from frappe import _, msgprint
from frappe.utils import cint, cstr, strip_html
from frappe.utils.nestedset import get_root_of
from shopify.collection import PaginatedIterator
from shopify.resources import Collect, CustomCollection, Product, Shop, Variant

from ecommerce_integrations.ecommerce_integrations.doctype.ecommerce_item import ecommerce_item
from ecommerce_integrations.shopify.connection import temp_shopify_session
from ecommerce_integrations.shopify.constants import (
	ITEM_SELLING_RATE_FIELD,
	MODULE_NAME,
	SETTING_DOCTYPE,
	SHOPIFY_VARIANTS_ATTR_LIST,
	SUPPLIER_ID_FIELD,
	WEIGHT_TO_ERPNEXT_UOM_MAP,
)
from ecommerce_integrations.shopify.utils import create_shopify_log


class ShopifyProduct:
	def __init__(
		self,
		product_id: str,
		variant_id: Optional[str] = None,
		sku: Optional[str] = None,
		has_variants: Optional[int] = 0,
	):
		self.product_id = str(product_id)
		self.variant_id = str(variant_id) if variant_id else None
		self.sku = str(sku) if sku else None
		self.has_variants = has_variants
		self.setting = frappe.get_doc(SETTING_DOCTYPE)

		if not self.setting.is_enabled():
			frappe.throw(_("Can not create Shopify product when integration is disabled."))

	def is_synced(self) -> bool:
		return ecommerce_item.is_synced(
			MODULE_NAME, integration_item_code=self.product_id, variant_id=self.variant_id, sku=self.sku,
		)

	def get_erpnext_item(self):
		return ecommerce_item.get_erpnext_item(
			MODULE_NAME,
			integration_item_code=self.product_id,
			variant_id=self.variant_id,
			sku=self.sku,
			has_variants=self.has_variants,
		)

	@temp_shopify_session
	def sync_product(self):
		if not self.is_synced():
			shopify_product = Product.find(self.product_id)
			product_dict = shopify_product.to_dict()
			self._make_item(product_dict)

	def _make_item(self, product_dict):
		_add_weight_details(product_dict)

		warehouse = self.setting.warehouse

		if _has_variants(product_dict):
			self.has_variants = 1
			attributes = self._create_attribute(product_dict)
			self._create_item(product_dict, warehouse, 1, attributes)
			self._create_item_variants(product_dict, warehouse, attributes)

		else:
			product_dict["variant_id"] = product_dict["variants"][0]["id"]
			self._create_item(product_dict, warehouse)

	def _create_attribute(self, product_dict):
		attribute = []
		for attr in product_dict.get("options"):
			if not frappe.db.get_value("Item Attribute", attr.get("name"), "name"):
				frappe.get_doc(
					{
						"doctype": "Item Attribute",
						"attribute_name": attr.get("name"),
						"item_attribute_values": [
							{"attribute_value": attr_value, "abbr": attr_value} for attr_value in attr.get("values")
						],
					}
				).insert()
				attribute.append({"attribute": attr.get("name")})

			else:
				# check for attribute values
				item_attr = frappe.get_doc("Item Attribute", attr.get("name"))
				if not item_attr.numeric_values:
					self._set_new_attribute_values(item_attr, attr.get("values"))
					item_attr.save()
					attribute.append({"attribute": attr.get("name")})

				else:
					attribute.append(
						{
							"attribute": attr.get("name"),
							"from_range": item_attr.get("from_range"),
							"to_range": item_attr.get("to_range"),
							"increment": item_attr.get("increment"),
							"numeric_values": item_attr.get("numeric_values"),
						}
					)

		return attribute

	def _set_new_attribute_values(self, item_attr, values):
		for attr_value in values:
			if not any(
				(d.abbr.lower() == attr_value.lower() or d.attribute_value.lower() == attr_value.lower())
				for d in item_attr.item_attribute_values
			):
				item_attr.append("item_attribute_values", {"attribute_value": attr_value, "abbr": attr_value})

	def _create_item(self, product_dict, warehouse, has_variant=0, attributes=None, variant_of=None):
		item_dict = {
			"variant_of": variant_of,
			"is_stock_item": 1,
			"item_code": cstr(product_dict.get("item_code")) or cstr(product_dict.get("id")),
			"item_name": product_dict.get("title", "").strip(),
			"description": product_dict.get("body_html") or product_dict.get("title"),
			"brand": product_dict.get("product_type"),			
			"has_variants": has_variant,
			"attributes": attributes or [],
			"stock_uom": product_dict.get("uom") or _("Nos"),
			"sku": product_dict.get("sku") or _get_sku(product_dict),
			"default_warehouse": warehouse,
			"image": _get_item_image(product_dict),
			"weight_uom": WEIGHT_TO_ERPNEXT_UOM_MAP[product_dict.get("weight_unit")],
			"weight_per_unit": product_dict.get("weight"),
			"default_supplier": self._get_supplier(product_dict),
		}

		integration_item_code = product_dict["id"]  # shopify product_id
		variant_id = product_dict.get("variant_id", "")  # shopify variant_id if has variants
		sku = item_dict["sku"]

		if not _match_sku_and_link_item(
			item_dict, integration_item_code, variant_id, variant_of=variant_of, has_variant=has_variant
		):
			ecommerce_item.create_ecommerce_item(
				MODULE_NAME,
				integration_item_code,
				item_dict,
				variant_id=variant_id,
				sku=sku,
				variant_of=variant_of,
				has_variants=has_variant,
			)

	def _create_item_variants(self, product_dict, warehouse, attributes):
		template_item = ecommerce_item.get_erpnext_item(
			MODULE_NAME, integration_item_code=product_dict.get("id"), has_variants=1
		)

		if template_item:
			for variant in product_dict.get("variants"):
				shopify_item_variant = {
					"id": product_dict.get("id"),
					"variant_id": variant.get("id"),
					"item_code": variant.get("id"),
					"title": product_dict.get("title", "").strip() + "-" + variant.get("title"),
					"product_type": product_dict.get("product_type"),
					"sku": variant.get("sku"),
					"uom": template_item.stock_uom or _("Nos"),
					"item_price": variant.get("price"),
					"weight_unit": variant.get("weight_unit"),
					"weight": variant.get("weight"),
				}

				for i, variant_attr in enumerate(SHOPIFY_VARIANTS_ATTR_LIST):
					if variant.get(variant_attr):
						attributes[i].update(
							{"attribute_value": self._get_attribute_value(variant.get(variant_attr), attributes[i])}
						)
				self._create_item(shopify_item_variant, warehouse, 0, attributes, template_item.name)

	def _get_attribute_value(self, variant_attr_val, attribute):
		attribute_value = frappe.db.sql(
			"""select attribute_value from `tabItem Attribute Value`
			where parent = %s and (abbr = %s or attribute_value = %s)""",
			(attribute["attribute"], variant_attr_val, variant_attr_val),
			as_list=1,
		)
		return attribute_value[0][0] if len(attribute_value) > 0 else cint(variant_attr_val)

	def _get_supplier(self, product_dict):
		if product_dict.get("vendor"):
			supplier = frappe.db.sql(
				f"""select name from tabSupplier
				where name = %s or {SUPPLIER_ID_FIELD} = %s """,
				(product_dict.get("vendor"), product_dict.get("vendor").lower()),
				as_list=1,
			)

			if supplier:
				return product_dict.get("vendor")
			supplier = frappe.get_doc(
				{
					"doctype": "Supplier",
					"supplier_name": product_dict.get("vendor"),
					SUPPLIER_ID_FIELD: product_dict.get("vendor").lower(),
					"supplier_group": self._get_supplier_group(),
				}
			).insert()
			return supplier.name
		else:
			return ""

	def _get_supplier_group(self):
		supplier_group = frappe.db.get_value("Supplier Group", _("Shopify Supplier"))
		if not supplier_group:
			supplier_group = frappe.get_doc(
				{"doctype": "Supplier Group", "supplier_group_name": _("Shopify Supplier")}
			).insert()
			return supplier_group.name
		return supplier_group


def _add_weight_details(product_dict):
	variants = product_dict.get("variants")
	if variants:
		product_dict["weight"] = variants[0]["weight"]
		product_dict["weight_unit"] = variants[0]["weight_unit"]


def _has_variants(product_dict) -> bool:
	options = product_dict.get("options")
	return bool(options and "Default Title" not in options[0]["values"])


def _get_sku(product_dict):
	if product_dict.get("variants"):
		return product_dict.get("variants")[0].get("sku")
	return ""


def _get_item_image(product_dict):
	if product_dict.get("image"):
		return product_dict.get("image").get("src")
	return None


def _match_sku_and_link_item(
	item_dict, product_id, variant_id, variant_of=None, has_variant=False
) -> bool:
	"""Tries to match new item with existing item using Shopify SKU == item_code.

	Returns true if matched and linked.
	"""
	sku = item_dict["sku"]
	if not sku or variant_of or has_variant:
		return False

	item_name = frappe.db.get_value("Item", {"item_code": sku})
	if item_name:
		try:
			ecommerce_item = frappe.get_doc(
				{
					"doctype": "Ecommerce Item",
					"integration": MODULE_NAME,
					"erpnext_item_code": item_name,
					"integration_item_code": product_id,
					"has_variants": 0,
					"variant_id": cstr(variant_id),
					"sku": sku,
				}
			)

			ecommerce_item.insert()
			return True
		except Exception:
			return False


def create_items_if_not_exist(order):
	"""Using shopify order, sync all items that are not already synced."""
	for item in order.get("line_items", []):

		product_id = item["product_id"]
		variant_id = item.get("variant_id")
		sku = item.get("sku")
		product = ShopifyProduct(product_id, variant_id=variant_id, sku=sku)

		if not product.is_synced():
			product.sync_product()


def get_item_code(shopify_item):
	"""Get item code using shopify_item dict.

	Item should contain both product_id and variant_id."""

	item = ecommerce_item.get_erpnext_item(
		integration=MODULE_NAME,
		integration_item_code=shopify_item.get("product_id"),
		variant_id=shopify_item.get("variant_id"),
		sku=shopify_item.get("sku"),
	)
	if item:
		return item.item_code


RETRY_METHOD = "ecommerce_integrations.shopify.product.retry_item_upload"
MAX_RETRY_ATTEMPTS = 5


class ShopifyUploadError(Exception):
	"""Shopify refused to save the product (validation errors)."""


def upload_erpnext_item(doc, method=None):
	"""This hook is called when inserting new or updating existing `Item`.

	New items are pushed to shopify and changes to existing items are
	updated depending on what is configured in "Shopify Setting" doctype.

	A Shopify failure never blocks the Item save (F2): the item is put on the
	retry list and sent by `retry_pending_item_uploads` once Shopify answers.
	"""
	if doc.flags.from_integration or frappe.flags.in_import:
		return

	# A new item fires after_insert and then on_update: send it once (F8)
	if method == "on_update" and doc.flags.in_insert:
		return

	# Nothing Shopify shows has changed: skip the whole upload (F8)
	if not has_shopify_changes(doc) and _is_linked(doc.name):
		return

	frappe.flags.in_shopify_item_upload = True
	try:
		uploaded = _upload_erpnext_item(doc, sync_collections=_collections_changed(doc))
	except Exception as e:
		if _is_database_error(e):
			raise
		frappe.clear_last_message()  # drop the red dialog a frappe.throw() may have queued
		_queue_item_retry(doc.name, e)
		msgprint(
			_("Saved, but not sent to Shopify: {0}<br>It will be sent automatically when Shopify is reachable.").format(
				frappe.utils.escape_html(_short_error(e))
			),
			title=_("Shopify"),
			indicator="orange",
		)
	else:
		if uploaded:
			_close_item_retry(doc.name, _("Sent on a later save of the item."))
	finally:
		frappe.flags.in_shopify_item_upload = False


# Item fields that end up in Shopify (product, variant or collections)
SHOPIFY_ITEM_FIELDS = (
	"item_name",
	"description",
	"brand",
	"disabled",
	"item_group",
	"gender",
	"weight_per_unit",
	"weight_uom",
	"is_stock_item",
	ITEM_SELLING_RATE_FIELD,
)


def _attribute_values(doc) -> list:
	return [(a.attribute, a.attribute_value) for a in doc.get("attributes") or []]


def has_shopify_changes(doc) -> bool:
	"""True when a field Shopify uses changed in this save (or it's not a save)."""
	before = doc.get_doc_before_save()
	if not before:
		return True
	if any(before.get(f) != doc.get(f) for f in SHOPIFY_ITEM_FIELDS):
		return True
	return _attribute_values(before) != _attribute_values(doc)


def _collections_changed(doc) -> bool:
	return doc.has_value_changed("item_group") or doc.has_value_changed("gender")


def _is_linked(item_code: str) -> bool:
	return bool(
		frappe.db.exists("Ecommerce Item", {"erpnext_item_code": item_code, "integration": MODULE_NAME})
	)


@temp_shopify_session
def _upload_erpnext_item(doc, sync_collections=True):
	"""Push the item to Shopify. Returns True when something was sent.

	Collections are only synced when `sync_collections` (Item Group or Gender
	changed, a new product, or a retry). Raises on any Shopify failure;
	callers decide whether to retry."""
	template_item = item = doc  # alias for readability

	setting = frappe.get_doc(SETTING_DOCTYPE)

	if not setting.is_enabled() or not setting.upload_erpnext_items:
		return False

	if item.has_variants:
		return False

	if len(item.attributes) > 3:
		msgprint(_("Template items/Items with 4 or more attributes can not be uploaded to Shopify."))
		return False

	if doc.variant_of and not setting.upload_variants_as_items:
		msgprint(_("Enable variant sync in setting to upload item to Shopify."))
		return False

	if item.variant_of:
		template_item = frappe.get_doc("Item", item.variant_of)

	product_id = frappe.db.get_value(
		"Ecommerce Item",
		{"erpnext_item_code": template_item.name, "integration": MODULE_NAME},
		"integration_item_code",
	)
	is_new_product = not bool(product_id)

	if is_new_product:
		product = Product()
		product.published = False
		product.status = "active" if setting.sync_new_item_as_active else "draft"

		map_erpnext_item_to_shopify(shopify_product=product, erpnext_item=template_item)
		_save_product(product)

		# Link the product straight away: if a later step fails, the retry
		# updates this product instead of creating a duplicate in Shopify.
		if item.variant_of:
			_insert_ecommerce_item(template_item, product)
		else:
			_insert_ecommerce_item(item, product, product.variants[0], sku=item.item_code)

		update_default_variant_properties(
			product,
			sku=template_item.item_code,
			price=template_item.get(ITEM_SELLING_RATE_FIELD) or 0,
			is_stock_item=template_item.is_stock_item,
		)
		if item.variant_of:
			product.options = []
			product.variants = []
			variant_attributes = {
				"title": template_item.item_name,
				"sku": item.item_code,
				"price": item.get(ITEM_SELLING_RATE_FIELD) or 0,
			}
			max_index_range = min(3, len(template_item.attributes))
			for i in range(0, max_index_range):
				attr = template_item.attributes[i]
				product.options.append(
					{
						"name": attr.attribute,
						"values": frappe.db.get_all(
							"Item Attribute Value", {"parent": attr.attribute}, pluck="attribute_value"
						),
					}
				)

			# Map variant attributes to options using the new intelligent mapping
			option_attributes = _map_variant_attributes_to_options(item, template_item)
			variant_attributes.update(option_attributes)

			add_or_update_variant(Variant(variant_attributes), product.variants)

		_save_product(product)  # push variant

		if item.variant_of:
			_insert_ecommerce_item(item, product, product.variants[0])

		sync_collections_from_erp_item(product.id, template_item)
		write_upload_log(status=True, product=product, item=item)
		return True

	if not setting.update_shopify_item_on_update:
		return False

	product = Product.find(product_id)
	if not product:
		return False

	map_erpnext_item_to_shopify(shopify_product=product, erpnext_item=template_item)
	if not item.variant_of:
		update_default_variant_properties(
			product, is_stock_item=template_item.is_stock_item, price=item.get(ITEM_SELLING_RATE_FIELD) or 0
		)
		# a product created by an interrupted first upload has no SKU yet
		if not product.variants[0].sku:
			product.variants[0].sku = item.item_code
	else:
		variant_attributes = {"sku": item.item_code, "price": item.get(ITEM_SELLING_RATE_FIELD) or 0}
		product.options = []
		max_index_range = min(3, len(template_item.attributes))
		for i in range(0, max_index_range):
			attr = template_item.attributes[i]
			product.options.append(
				{
					"name": attr.attribute,
					"values": frappe.db.get_all(
						"Item Attribute Value", {"parent": attr.attribute}, pluck="attribute_value"
					),
				}
			)

		# Map variant attributes to options using the new intelligent mapping
		option_attributes = _map_variant_attributes_to_options(item, template_item)
		variant_attributes.update(option_attributes)

		add_or_update_variant(Variant(variant_attributes), product.variants)

	_save_product(product)
	if sync_collections:
		sync_collections_from_erp_item(product.id, template_item)
	if item.variant_of:
		map_erpnext_variant_to_shopify_variant(product, item, variant_attributes)

	write_upload_log(status=True, product=product, item=item, action="Updated")
	return True


def _save_product(product: Product) -> None:
	if not product.save():
		raise ShopifyUploadError(", ".join(product.errors.full_messages()) or "Unknown error")


def _insert_ecommerce_item(erpnext_item, product: Product, variant: Optional[Variant] = None, sku=None):
	frappe.get_doc(
		{
			"doctype": "Ecommerce Item",
			"erpnext_item_code": erpnext_item.name,
			"integration": MODULE_NAME,
			"integration_item_code": str(product.id),
			"variant_id": str(variant.id) if variant else "",
			"sku": sku or (str(variant.sku or "") if variant else ""),
			"has_variants": erpnext_item.has_variants,
			"variant_of": erpnext_item.variant_of,
		}
	).insert()


# ---------------------------------------------------------------------------
# Retry list (F2): failed uploads are Ecommerce Integration Log rows with
# method RETRY_METHOD, status "Error" and request_data {"item_code": ...}.
# The log's Retry button re-runs them through the same function.
# ---------------------------------------------------------------------------


def _retry_request_data(item_code: str) -> str:
	return json.dumps({"item_code": item_code})


def _get_open_retry(item_code: str) -> Optional[str]:
	return frappe.db.get_value(
		"Ecommerce Integration Log",
		{
			"integration": MODULE_NAME,
			"method": RETRY_METHOD,
			"status": ["in", ["Error", "Queued"]],
			"request_data": _retry_request_data(item_code),
		},
	)


def _queue_item_retry(item_code: str, error: Exception) -> None:
	"""Add the item to the retry list (one row per item), inside the save's own
	transaction: no mid-save commit, and nothing is queued if the save fails."""
	name = _get_open_retry(item_code)
	log = (
		frappe.get_doc("Ecommerce Integration Log", name)
		if name
		else frappe.get_doc(
			{
				"doctype": "Ecommerce Integration Log",
				"integration": MODULE_NAME,
				"method": RETRY_METHOD,
				"request_data": _retry_request_data(item_code),
				"response_data": json.dumps({"attempts": 0}),
			}
		)
	)
	log.status = "Error"
	log.response_data = json.dumps({"attempts": 0})
	log.message = _("Waiting to be sent to Shopify. Item {0}: {1}").format(item_code, _short_error(error))
	log.traceback = frappe.get_traceback()
	log.save(ignore_permissions=True)


def _close_item_retry(item_code: str, message: str) -> None:
	name = _get_open_retry(item_code)
	if name:
		frappe.db.set_value(
			"Ecommerce Integration Log", name, {"status": "Success", "message": message, "traceback": ""}
		)


def _get_attempts(log) -> int:
	try:
		return cint(json.loads(log.response_data or "{}").get("attempts"))
	except ValueError:
		return 0


def retry_item_upload(payload, request_id=None):
	"""Send one waiting item. Used by the scheduler and the log's Retry button.

	Returns False when Shopify itself is failing (stop sending more items)."""
	if isinstance(payload, str):
		payload = json.loads(payload)
	item_code = payload.get("item_code")

	log = frappe.get_doc("Ecommerce Integration Log", request_id) if request_id else None
	if not log:
		name = _get_open_retry(item_code)
		log = frappe.get_doc("Ecommerce Integration Log", name) if name else None

	if not frappe.db.exists("Item", item_code):
		if log:
			log.db_set({"status": "Invalid", "message": _("Item {0} no longer exists.").format(item_code)})
		frappe.db.commit()
		return True

	previous_attempts = _get_attempts(log) if log else 0
	attempts = previous_attempts + 1
	frappe.flags.in_shopify_item_upload = True
	try:
		_upload_erpnext_item(frappe.get_doc("Item", item_code))
	except Exception as e:
		# Keep a product link saved before the failure, so the next try
		# updates that product instead of creating a duplicate.
		if _is_database_error(e):
			frappe.db.rollback()
		frappe.clear_last_message()
		shop_wide = _is_shop_wide_error(e)
		if shop_wide:
			attempts = previous_attempts  # not this item's fault: don't use up a try
		if log:
			log.reload()
			log.db_set(
				{
					"status": "Error",
					"message": _("Waiting to be sent to Shopify (attempt {0} of {1}). Item {2}: {3}").format(
						attempts, MAX_RETRY_ATTEMPTS, item_code, _short_error(e)
					),
					"traceback": frappe.get_traceback(),
					"response_data": json.dumps({"attempts": attempts}),
				}
			)
		return not shop_wide
	else:
		if log:
			log.db_set(
				{
					"status": "Success",
					"message": _("Sent to Shopify on retry (attempt {0}).").format(attempts),
					"traceback": "",
					"response_data": json.dumps({"attempts": attempts}),
				}
			)
	finally:
		frappe.flags.in_shopify_item_upload = False
		frappe.db.commit()
	return True


def retry_pending_item_uploads():
	"""Scheduler (every 30 min): send waiting items once Shopify is reachable."""
	setting = frappe.get_doc(SETTING_DOCTYPE)
	if not setting.is_enabled() or not setting.upload_erpnext_items:
		return

	pending = [
		log
		for log in frappe.get_all(
			"Ecommerce Integration Log",
			filters={"integration": MODULE_NAME, "method": RETRY_METHOD, "status": "Error"},
			fields=["name", "request_data", "response_data"],
			order_by="creation asc",
		)
		if _get_attempts(log) < MAX_RETRY_ATTEMPTS
	]
	if not pending or not _shopify_is_reachable():
		return

	for i, log in enumerate(pending):
		if i:
			time.sleep(1)  # Shopify allows about 2 calls per second
		if not retry_item_upload(log.request_data, request_id=log.name):
			break  # Shopify-wide problem: try the rest on the next run


@temp_shopify_session
def _check_shop():
	Shop.current()


def _shopify_is_reachable() -> bool:
	"""One cheap call: an outage costs nothing and no item uses up an attempt."""
	frappe.flags.in_shopify_item_upload = True  # the outage is expected here: no log row
	try:
		_check_shop()
		return True
	except Exception:
		frappe.clear_last_message()
		return False
	finally:
		frappe.flags.in_shopify_item_upload = False


def _is_shop_wide_error(error: Exception) -> bool:
	"""Outage, rate limit, auth or network problem, as opposed to Shopify
	rejecting this particular item."""
	from pyactiveresource import connection as conn

	from ecommerce_integrations.shopify.connection import ShopifyShopUnavailable

	if isinstance(error, (ShopifyShopUnavailable, conn.ServerError, socket.timeout, TimeoutError)):
		return True
	if isinstance(error, (conn.UnauthorizedAccess, conn.ForbiddenAccess)):
		return True
	if isinstance(error, conn.ClientError):
		return cint(getattr(getattr(error, "response", None), "code", 0)) in (402, 423, 429)
	# pyactiveresource raises the bare Error class for network failures
	return type(error) in (conn.Error, conn.ConnectionError)


def _is_database_error(error: Exception) -> bool:
	import pymysql

	return isinstance(error, pymysql.err.Error)


def _short_error(error: Exception) -> str:
	text = strip_html(cstr(error)).strip() or error.__class__.__name__
	return text if len(text) <= 300 else text[:300] + "…"


def _get_variant_attribute_value(item, attribute_name, index):
	"""Safely get variant attribute value, handling cases where variant doesn't have all template attributes."""
	try:
		# First try to find the attribute by name in the item's attributes
		for attr in item.attributes:
			if attr.attribute == attribute_name:
				return attr.attribute_value
		
		# If not found by name, try by index (fallback for backward compatibility)
		if index < len(item.attributes):
			return item.attributes[index].attribute_value
		
		# If still not found, return None (will be handled as empty option)
		return None
	except (IndexError, AttributeError):
		return None

def _map_variant_attributes_to_options(item, template_item):
	"""Map variant attributes to Shopify option structure, handling missing attributes intelligently."""
	variant_attributes = {}
	
	# Create a mapping of attribute names to their values for this variant
	variant_attr_map = {}
	for attr in item.attributes:
		variant_attr_map[attr.attribute] = attr.attribute_value
	
	# Map each template attribute to the correct option position
	for i, template_attr in enumerate(template_item.attributes):
		option_key = f"option{i+1}"
		attr_name = template_attr.attribute
		
		# First try to get the value from this variant
		if attr_name in variant_attr_map and variant_attr_map[attr_name]:
			variant_attributes[option_key] = variant_attr_map[attr_name]
		else:
			# If not found, try to get default from other variants
			default_value = _get_default_attribute_value_from_template(template_item, attr_name, i)
			if default_value:
				variant_attributes[option_key] = default_value
			else:
				# Last resort: use the first available value from the attribute definition
				try:
					attr_values = frappe.db.get_all(
						"Item Attribute Value", 
						{"parent": attr_name}, 
						pluck="attribute_value"
					)
					if attr_values:
						variant_attributes[option_key] = attr_values[0]
					else:
						# If still no value, use a generic fallback
						variant_attributes[option_key] = f"Default {attr_name}"
				except Exception:
					variant_attributes[option_key] = f"Default {attr_name}"
	
	return variant_attributes

def _get_default_attribute_value_from_template(template_item, attribute_name, index):
	"""Get a default attribute value from other variants of the same template."""
	try:
		# Get all variants of this template
		variants = frappe.get_all(
			"Item",
			filters={"variant_of": template_item.name},
			fields=["name"]
		)
		
		# Look for the first variant that has this attribute
		for variant in variants:
			try:
				variant_doc = frappe.get_doc("Item", variant.name)
				if variant_doc.attributes:
					# Find the attribute by name
					for attr in variant_doc.attributes:
						if attr.attribute == attribute_name:
							return attr.attribute_value
					
					# Fallback: try by index
					if index < len(variant_doc.attributes):
						return variant_doc.attributes[index].attribute_value
			except Exception:
				continue
		
		# If no variant has this attribute, try to get a default from the attribute values
		try:
			attr_values = frappe.db.get_all(
				"Item Attribute Value", 
				{"parent": attribute_name}, 
				pluck="attribute_value"
			)
			if attr_values:
				return attr_values[0]  # Return the first available value
		except Exception:
			pass
		
		return None
	except (Exception, AttributeError):
		return None


def add_or_update_variant(new_variant: Variant, variants: List[Variant]):
	# Drop only Shopify's placeholder variant. Variants without a SKU are real
	# variants (e.g. added in Shopify) and Shopify deletes any variant missing
	# from a product save (F4).
	variants[:] = [v for v in variants if getattr(v, "option1", "") != "Default Title"]
	for variant in variants:
		if variant.sku == new_variant.sku:
			variant.price = new_variant.price 
			return  
	variants.append(new_variant)


def map_erpnext_variant_to_shopify_variant(
	shopify_product: Product, erpnext_item, variant_attributes
):
	variant_product_id = frappe.db.get_value(
		"Ecommerce Item",
		{"erpnext_item_code": erpnext_item.name, "integration": MODULE_NAME},
		"integration_item_code",
	)
	if not variant_product_id:
		for variant in shopify_product.variants:
			if (
				variant.option1 == variant_attributes.get("option1")
				and variant.option2 == variant_attributes.get("option2")
				and variant.option3 == variant_attributes.get("option3")
			):
				variant_product_id = str(variant.id)
				if not frappe.flags.in_test:
					frappe.get_doc(
						{
							"doctype": "Ecommerce Item",
							"erpnext_item_code": erpnext_item.name,
							"integration": MODULE_NAME,
							"integration_item_code": str(shopify_product.id),
							"variant_id": variant_product_id,
							"sku": str(variant.sku),
							"variant_of": erpnext_item.variant_of,
						}
					).insert()
				break
		if not variant_product_id:
			msgprint(_("Shopify: Couldn't sync item variant."))
	return variant_product_id





# ---------------------------------------------------------------------------
# Collections (F6/F11). Item Group (+ parents) and Gender become Shopify custom
# collections. Only the difference is sent, and only collections whose title is
# an Item Group or Gender are ever removed: collections assigned by hand in
# Shopify are left alone. These helpers run inside the session opened by
# _upload_erpnext_item, so they carry no session decorator of their own.
# ---------------------------------------------------------------------------

CACHE_DURATION = 300  # seconds the list of Shopify collections is reused
_collection_index = {"by_title": {}, "by_id": {}, "loaded_at": None}


def _log_collection_error(message: str) -> None:
	"""Record a collection failure without committing mid-save (F7)."""
	frappe.log_error(message=message, title="Shopify Collections")


def clear_collection_cache():
	"""Force the next lookup to re-read the collections from Shopify."""
	_collection_index.update({"by_title": {}, "by_id": {}, "loaded_at": None})


def _get_collection_index() -> dict:
	"""All custom collections, read once per CACHE_DURATION (one call per 250)."""
	loaded_at = _collection_index["loaded_at"]
	if loaded_at is None or time.time() - loaded_at > CACHE_DURATION:
		by_title, by_id = {}, {}
		for page in PaginatedIterator(CustomCollection.find(limit=250)):
			for col in page:
				title = (col.title or "").strip()
				by_id[int(col.id)] = title
				by_title.setdefault(title.lower(), int(col.id))
		_collection_index.update({"by_title": by_title, "by_id": by_id, "loaded_at": time.time()})
	return _collection_index


def _remember_collection(collection_id: int, title: str) -> None:
	_collection_index["by_title"][title.lower()] = int(collection_id)
	_collection_index["by_id"][int(collection_id)] = title


def get_or_create_shopify_collection(title: str) -> Optional[int]:
	"""Id of the custom collection with this title (any case), creating it if needed."""
	title = (title or "").strip()
	if not title:
		return None

	try:
		collection_id = _get_collection_index()["by_title"].get(title.lower())
		if collection_id:
			return collection_id

		# The list may be up to CACHE_DURATION old: ask Shopify before creating
		existing = CustomCollection.find(title=title)
		if existing:
			_remember_collection(existing[0].id, existing[0].title or title)
			return int(existing[0].id)

		collection = CustomCollection()
		collection.title = title
		collection.published = True
		if collection.save():
			_remember_collection(collection.id, title)
			return int(collection.id)

		errors = collection.errors.full_messages() if getattr(collection, "errors", None) else "Unknown error"
		_log_collection_error(f"Failed to create collection '{title}': {errors}")
	except Exception as e:
		_log_collection_error(f"Error finding or creating collection '{title}': {e}")
	return None


def get_all_parent_groups(item_group: str) -> List[str]:
	parents: List[str] = []

	def recurse(group: str):
		parent = frappe.db.get_value("Item Group", group, "parent_item_group")
		if parent and parent != "All Item Groups" and parent not in parents:
			parents.append(parent)
			recurse(parent)

	recurse(item_group)
	return parents


def get_collection_titles_for_item(erpnext_item) -> List[str]:
	"""Collections the product should be in: Item Group, its parents, Gender."""
	titles = []
	if erpnext_item.item_group:
		titles.append(erpnext_item.item_group)
		titles.extend(get_all_parent_groups(erpnext_item.item_group))

	gender = erpnext_item.get("gender")
	if gender and gender.strip():
		titles.append(gender)

	unique = {}
	for title in titles:
		title = (title or "").strip()
		if title:
			unique.setdefault(title.lower(), title)
	return list(unique.values())


def is_erp_managed_collection(title: Optional[str]) -> bool:
	"""True when the title is an Item Group or a Gender (the database compares
	without case), i.e. a collection this integration creates."""
	title = (title or "").strip()
	if not title:
		return False
	if frappe.db.exists("Item Group", title):
		return True
	return bool(frappe.db.exists("DocType", "Gender") and frappe.db.exists("Gender", title))


def sync_collections_from_erp_item(product_id: int, erpnext_item) -> None:
	"""Add the product to missing collections and remove it from ERP-managed
	collections it no longer belongs to. Nothing else is touched."""
	wanted = {}
	for title in get_collection_titles_for_item(erpnext_item):
		collection_id = get_or_create_shopify_collection(title)
		if collection_id:
			wanted[int(collection_id)] = title

	try:
		current = {int(c.collection_id): c for c in Collect.find(product_id=product_id, limit=250)}
	except Exception as e:
		_log_collection_error(f"Failed to read collections of product {product_id}: {e}")
		return

	for collection_id in wanted.keys() - current.keys():
		try:
			collect = Collect({"product_id": product_id, "collection_id": collection_id})
			if not collect.save():
				errors = collect.errors.full_messages() if getattr(collect, "errors", None) else "Unknown error"
				_log_collection_error(
					f"Failed to add product {product_id} to collection '{wanted[collection_id]}': {errors}"
				)
		except Exception as e:
			_log_collection_error(f"Failed to add product {product_id} to collection '{wanted[collection_id]}': {e}")

	stale = current.keys() - wanted.keys()
	if not stale:
		return

	try:
		titles = _get_collection_index()["by_id"]
	except Exception as e:
		_log_collection_error(f"Failed to read collection titles: {e}")
		return

	for collection_id in stale:
		if not is_erp_managed_collection(titles.get(collection_id)):
			continue  # assigned by hand in Shopify (or unknown): keep it
		try:
			current[collection_id].destroy()
		except Exception as e:
			_log_collection_error(
				f"Failed to remove product {product_id} from collection '{titles.get(collection_id)}': {e}"
			)



def map_erpnext_item_to_shopify(shopify_product: Product, erpnext_item):
	"""Map erpnext fields to shopify, called both when updating and creating new products."""

	shopify_product.title = erpnext_item.item_name
	shopify_product.body_html = erpnext_item.description


	shopify_product.product_type = erpnext_item.brand


		

	if erpnext_item.weight_uom in WEIGHT_TO_ERPNEXT_UOM_MAP.values():
		# reverse lookup for key
		uom = get_shopify_weight_uom(erpnext_weight_uom=erpnext_item.weight_uom)
		shopify_product.weight = erpnext_item.weight_per_unit
		shopify_product.weight_unit = uom

	if erpnext_item.disabled:
		shopify_product.status = "draft"
		shopify_product.published = False
		msgprint(_("Status of linked Shopify product is changed to Draft."))


def get_shopify_weight_uom(erpnext_weight_uom: str) -> str:
	for shopify_uom, erpnext_uom in WEIGHT_TO_ERPNEXT_UOM_MAP.items():
		if erpnext_uom == erpnext_weight_uom:
			return shopify_uom


def update_default_variant_properties(
	shopify_product: Product,
	is_stock_item: bool,
	sku: Optional[str] = None,
	price: Optional[float] = None,
):
	"""Shopify creates default variant upon saving the product.

	Some item properties are supposed to be updated on the default variant.
	Input: saved shopify_product, sku and price
	"""
	default_variant: Variant = shopify_product.variants[0]

	# this will create Inventory item and qty will be updated by scheduled job.
	if is_stock_item:
		default_variant.inventory_management = "shopify"

	if price is not None:
		default_variant.price = price
	if sku is not None:
		default_variant.sku = sku


def write_upload_log(status: bool, product: Product, item, action="Created") -> None:
	if not status:
		msg = _("Failed to upload item to Shopify") + "<br>"
		msg += _("Shopify reported errors:") + " " + ", ".join(product.errors.full_messages())
		msgprint(msg, title="Note", indicator="orange")

		create_shopify_log(
			status="Error", request_data=product.to_dict(), message=msg, method="upload_erpnext_item",
		)
	else:
		create_shopify_log(
			status="Success",
			request_data=product.to_dict(),
			message=f"{action} Item: {item.name}, shopify product: {product.id}",
			method="upload_erpnext_item",
		)