import time
from collections import Counter

import frappe
from frappe.utils import cint, create_batch, now
from pyactiveresource.connection import ClientError, ResourceNotFound
from shopify.base import ShopifyResource
from shopify.resources import InventoryLevel, Variant

from ecommerce_integrations.controllers.inventory import (
	get_inventory_levels,
	update_inventory_sync_status,
)
from ecommerce_integrations.controllers.scheduling import need_to_run
from ecommerce_integrations.shopify.connection import temp_shopify_session
from ecommerce_integrations.shopify.constants import MODULE_NAME, SETTING_DOCTYPE
from ecommerce_integrations.shopify.utils import create_shopify_log


INVENTORY_JOB_NAME = "shopify.inventory.upload"
MAX_RATE_LIMIT_RETRIES = 5


def update_inventory_on_shopify() -> None:
	"""Upload stock levels from ERPNext to Shopify.

	Called by scheduler on configured interval. The upload itself runs in the
	long queue: paced to Shopify's rate limit, a large batch outlasts the
	default queue's timeout (F9).
	"""
	setting = frappe.get_doc(SETTING_DOCTYPE)

	if not setting.is_enabled() or not setting.update_erpnext_stock_levels_to_shopify:
		return

	if _is_inventory_upload_queued():
		return

	if not need_to_run(SETTING_DOCTYPE, "inventory_sync_frequency", "last_inventory_sync"):
		return

	frappe.enqueue(
		"ecommerce_integrations.shopify.inventory.run_inventory_upload",
		queue="long",
		timeout=3600,
		job_name=INVENTORY_JOB_NAME,
	)


def _is_inventory_upload_queued() -> bool:
	from frappe.utils.background_jobs import get_jobs

	site = frappe.local.site
	return INVENTORY_JOB_NAME in get_jobs(site=site, queue="long", key="job_name").get(site, [])


def run_inventory_upload() -> None:
	setting = frappe.get_doc(SETTING_DOCTYPE)
	if not setting.is_enabled() or not setting.update_erpnext_stock_levels_to_shopify:
		return

	warehous_map = setting.get_erpnext_to_integration_wh_mapping()
	inventory_levels = get_inventory_levels(tuple(warehous_map.keys()), MODULE_NAME)

	if inventory_levels:
		upload_inventory_data_to_shopify(inventory_levels, warehous_map)


@temp_shopify_session
def upload_inventory_data_to_shopify(inventory_levels, warehous_map) -> None:
	synced_on = now()
	# one item has a row per mapped warehouse: look its inventory id up once
	inventory_ids = {}
	# a disabled item must stay unsellable: always send 0 for it (F12)
	disabled_items = set(
		frappe.get_all(
			"Item",
			filters={"name": ["in", list({d.item_code for d in inventory_levels})], "disabled": 1},
			pluck="name",
		)
	)

	for inventory_sync_batch in create_batch(inventory_levels, 50):
		for d in inventory_sync_batch:
			d.shopify_location_id = warehous_map[d.warehouse]

			try:
				inventory_id = _get_inventory_item_id(d, inventory_ids)

				_call_shopify(
					InventoryLevel.set,
					location_id=d.shopify_location_id,
					inventory_item_id=inventory_id,
					# shopify doesn't support fractional quantity
					available=0
					if d.item_code in disabled_items
					else cint(d.actual_qty) - cint(d.reserved_qty),
				)
				update_inventory_sync_status(d.ecom_item, time=synced_on)
				d.status = "Success"
			except ResourceNotFound:
				# Variant or location is deleted, mark as last synced and ignore.
				update_inventory_sync_status(d.ecom_item, time=synced_on)
				d.status = "Not Found"
			except Exception as e:
				d.status = "Failed"
				d.failure_reason = str(e)

			frappe.db.commit()

		_log_inventory_update_status(inventory_sync_batch)


def _get_inventory_item_id(d, inventory_ids: dict) -> str:
	"""Shopify inventory item of the variant: stored on the Ecommerce Item after
	the first lookup, so later syncs need one call per level instead of two."""
	inventory_id = d.inventory_item_id or inventory_ids.get(d.ecom_item)
	if not inventory_id:
		variant = _call_shopify(Variant.find, d.variant_id)
		inventory_id = str(variant.inventory_item_id)
		frappe.db.set_value(
			"Ecommerce Item", d.ecom_item, "inventory_item_id", inventory_id, update_modified=False
		)
	inventory_ids[d.ecom_item] = inventory_id
	return inventory_id


def _call_shopify(fn, *args, **kwargs):
	"""Call Shopify, slowing down as the call allowance fills up and waiting
	out 429 "too many requests" answers instead of failing the item."""
	for attempt in range(MAX_RATE_LIMIT_RETRIES + 1):
		try:
			result = fn(*args, **kwargs)
		except ClientError as e:
			response = getattr(e, "response", None)
			if getattr(response, "code", None) != 429 or attempt == MAX_RATE_LIMIT_RETRIES:
				raise
			time.sleep(_retry_after(response))
			continue
		_pace()
		return result


def _header(headers, name: str):
	name = name.lower()
	for key, value in (headers or {}).items():
		if key.lower() == name:
			return value


def _retry_after(response) -> float:
	try:
		return min(max(float(_header(response.headers, "Retry-After")), 1.0), 10.0)
	except (TypeError, ValueError):
		return 2.0


def _pace() -> None:
	"""Shopify REST: a bucket of 40 calls that empties at 2 per second. The
	"X-Shopify-Shop-Api-Call-Limit: used/size" header says how full it is."""
	response = getattr(ShopifyResource.connection, "response", None)
	limit = _header(getattr(response, "headers", None), "X-Shopify-Shop-Api-Call-Limit")
	try:
		used, size = (int(x) for x in str(limit).split("/"))
	except ValueError:
		return
	if used >= size * 0.8:
		time.sleep(1)


def _log_inventory_update_status(inventory_levels) -> None:
	"""Create log of inventory update."""
	log_message = "variant_id,location_id,status,failure_reason\n"

	log_message += "\n".join(
		f"{d.variant_id},{d.shopify_location_id},{d.status},{d.failure_reason or ''}"
		for d in inventory_levels
	)

	stats = Counter([d.status for d in inventory_levels])

	percent_successful = stats["Success"] / len(inventory_levels)

	if percent_successful == 0:
		status = "Failed"
	elif percent_successful < 1:
		status = "Partial Success"
	else:
		status = "Success"

	log_message = f"Updated {percent_successful * 100}% items\n\n" + log_message

	create_shopify_log(method="update_inventory_on_shopify", status=status, message=log_message)
