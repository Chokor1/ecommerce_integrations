from typing import Optional, List

import frappe
from frappe import _, msgprint
from frappe.utils import cint, cstr
from frappe.utils.nestedset import get_root_of
from shopify.resources import Product, Variant, CustomCollection, Collect

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


@temp_shopify_session
def upload_erpnext_item(doc, method=None):
	"""This hook is called when inserting new or updating existing `Item`.

	New items are pushed to shopify and changes to existing items are
	updated depending on what is configured in "Shopify Setting" doctype.
	"""
	template_item = item = doc  # alias for readability
	# a new item recieved from ecommerce_integrations is being inserted
	if item.flags.from_integration:
		return

	setting = frappe.get_doc(SETTING_DOCTYPE)

	if not setting.is_enabled() or not setting.upload_erpnext_items:
		return

	if frappe.flags.in_import:
		return

	if item.has_variants:
		return

	if len(item.attributes) > 3:
		msgprint(_("Template items/Items with 4 or more attributes can not be uploaded to Shopify."))
		return

	if doc.variant_of and not setting.upload_variants_as_items:
		msgprint(_("Enable variant sync in setting to upload item to Shopify."))
		return

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
		is_successful = product.save()

		if is_successful:
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

			product.save()  # push variant
			sync_collections_from_erp_item(product.id, template_item)

			ecom_items = list(set([item, template_item]))
			for d in ecom_items:
				ecom_item = frappe.get_doc(
					{
						"doctype": "Ecommerce Item",
						"erpnext_item_code": d.name,
						"integration": MODULE_NAME,
						"integration_item_code": str(product.id),
						"variant_id": "" if d.has_variants else str(product.variants[0].id),
						"sku": "" if d.has_variants else str(product.variants[0].sku),
						"has_variants": d.has_variants,
						"variant_of": d.variant_of,
					}
				)
				ecom_item.insert()

		write_upload_log(status=is_successful, product=product, item=item)
	elif setting.update_shopify_item_on_update:
		product = Product.find(product_id)
		if product:
			map_erpnext_item_to_shopify(shopify_product=product, erpnext_item=template_item)
			if not item.variant_of:
				update_default_variant_properties(
					product, is_stock_item=template_item.is_stock_item, price=item.get(ITEM_SELLING_RATE_FIELD) or 0
				)
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
				
				# product.variants.append(Variant(variant_attributes))
				add_or_update_variant(Variant(variant_attributes), product.variants)
			is_successful = product.save()
			if is_successful:
				unlink_collections_from_product(product.id)
				sync_collections_from_erp_item(product.id, template_item)
			if is_successful and item.variant_of:
				map_erpnext_variant_to_shopify_variant(product, item, variant_attributes)

			write_upload_log(status=is_successful, product=product, item=item, action="Updated")


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
	variants[:] = [
		v for v in variants
		if not (getattr(v, "option1", "") == "Default Title" or getattr(v, "sku", "") == "")
	]
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





# Cache to store recently fetched collections to avoid repeated API calls
_collection_cache = {}
_cache_timestamp = None
CACHE_DURATION = 300  # 5 minutes

def clear_collection_cache():
	"""Clear the collection cache to force fresh data fetch."""
	global _collection_cache, _cache_timestamp
	_collection_cache = {}
	_cache_timestamp = None
	create_shopify_log(message="Collection cache cleared")

@temp_shopify_session
def get_collection_by_title(title):
	"""Get a specific collection by title using direct search."""
	try:
		collections = CustomCollection.find(title=title)
		if collections:
			create_shopify_log(message=f"Found collection '{title}' (ID: {collections[0].id})")
			return collections[0]
		else:
			create_shopify_log(message=f"No collection found with title '{title}'")
			return None
	except Exception as e:
		create_shopify_log(message=f"Error searching for collection '{title}': {str(e)}", error=True)
		return None

@temp_shopify_session
def find_collection_case_insensitive(target_title):
	"""Find collection with case-insensitive matching."""
	try:
		all_collections = CustomCollection.find()
		if all_collections:
			for col in all_collections:
				if hasattr(col, 'title') and col.title:
					col_title = col.title.strip()
					if col_title.lower() == target_title.lower():
						create_shopify_log(message=f"Found collection (case-insensitive): '{col_title}' (ID: {col.id})")
						return col
		return None
	except Exception as e:
		create_shopify_log(message=f"Error in case-insensitive search for '{target_title}': {str(e)}", error=True)
		return None

#update by abbass
@temp_shopify_session
def get_or_create_shopify_collection(title: str) -> Optional[int]:
	"""Fetch or create a Shopify CustomCollection by title."""
	if not title or not title.strip():
		create_shopify_log(message="Empty collection title provided", error=True)
		return None
	
	# Normalize the title for comparison
	normalized_title = title.strip()
	
	# Check cache first
	import time
	global _collection_cache, _cache_timestamp
	
	current_time = time.time()
	if (_cache_timestamp is None or current_time - _cache_timestamp > CACHE_DURATION):
		_collection_cache = {}
		_cache_timestamp = current_time
		create_shopify_log(message="Collection cache expired, fetching fresh data")
	
	# Check if we have this collection in cache
	if normalized_title.lower() in _collection_cache:
		cached_id = _collection_cache[normalized_title.lower()]
		create_shopify_log(message=f"Found collection '{normalized_title}' in cache (ID: {cached_id})")
		return cached_id
	
	try:
		# Search for existing collection by title (case-sensitive search first)
		existing_collection = get_collection_by_title(normalized_title)
		if existing_collection:
			# Store in cache for future use
			_collection_cache[normalized_title.lower()] = existing_collection.id
			create_shopify_log(message=f"Found existing collection: {existing_collection.title} (ID: {existing_collection.id})")
			return existing_collection.id
		
		# If not found with exact match, try case-insensitive search
		create_shopify_log(message=f"No exact match found for '{normalized_title}', trying case-insensitive search")
		existing_collection = find_collection_case_insensitive(normalized_title)
		if existing_collection:
			# Store in cache for future use
			_collection_cache[normalized_title.lower()] = existing_collection.id
			create_shopify_log(message=f"Found existing collection (case-insensitive): {existing_collection.title} (ID: {existing_collection.id})")
			return existing_collection.id
		
		create_shopify_log(message=f"No existing collection found for '{normalized_title}', creating new one")

		# Create new collection if not found
		collection = CustomCollection()
		collection.title = normalized_title
		collection.published = True
		
		try:
			if collection.save():
				# Store in cache for future use
				_collection_cache[normalized_title.lower()] = collection.id
				create_shopify_log(message=f"Successfully created new collection: {normalized_title} (ID: {collection.id})")
				return collection.id
			else:
				error_msg = "Unknown error"
				if hasattr(collection, 'errors') and collection.errors:
					error_msg = collection.errors.full_messages()
				create_shopify_log(message=f"Failed to create collection '{normalized_title}': {error_msg}", error=True)
				return None
		except Exception as save_error:
			create_shopify_log(message=f"Exception during collection save for '{normalized_title}': {str(save_error)}", error=True)
			return None
			
	except Exception as e:
		create_shopify_log(message=f"Error in get_or_create_shopify_collection for '{normalized_title}': {str(e)}", error=True)
		return None


@temp_shopify_session
def link_product_to_collections(product_id: int, collection_titles: List[str]):
	"""Link a product to multiple collections, avoiding duplicates."""
	if not collection_titles:
		return
		
	# Remove duplicates and empty titles
	unique_titles = list(set([title.strip() for title in collection_titles if title and title.strip()]))
	
	if not unique_titles:
		create_shopify_log(message="No valid collection titles provided for linking")
		return
	
	create_shopify_log(message=f"Linking product {product_id} to collections: {unique_titles}")
	
	for title in unique_titles:
		collection_id = get_or_create_shopify_collection(title)
		if not collection_id:
			create_shopify_log(message=f"Failed to get/create collection '{title}', skipping link", error=True)
			continue

		try:
			# Check if the product is already linked to this collection
			existing_collects = []
			try:
				existing_collects = Collect.find(product_id=product_id, collection_id=collection_id)
			except Exception as find_error:
				create_shopify_log(message=f"Error checking existing collects for product {product_id}, collection {collection_id}: {str(find_error)}", error=True)
				# Continue with creation attempt even if we can't check existing links
			
			if existing_collects:
				create_shopify_log(message=f"Product {product_id} already linked to collection '{title}' (ID: {collection_id})")
				continue
				
			# Create new collect link
			collect = Collect()
			collect.product_id = product_id
			collect.collection_id = collection_id
			
			try:
				if collect.save():
					create_shopify_log(message=f"Successfully linked product {product_id} to collection '{title}' (ID: {collection_id})")
				else:
					error_msg = "Unknown error"
					if hasattr(collect, 'errors') and collect.errors:
						error_msg = collect.errors.full_messages()
					create_shopify_log(message=f"Failed to save collect link for product {product_id} to collection '{title}': {error_msg}", error=True)
			except Exception as save_error:
				create_shopify_log(message=f"Exception during collect save for product {product_id} to collection '{title}': {str(save_error)}", error=True)
				
		except Exception as e:
			create_shopify_log(message=f"Failed to link product {product_id} to collection '{title}': {str(e)}", error=True)


def get_all_parent_groups(item_group: str) -> List[str]:
	parents: List[str] = []

	def recurse(group: str):
		parent = frappe.db.get_value("Item Group", group, "parent_item_group")
		if parent and parent != "All Item Groups":
			parents.append(parent)
			recurse(parent)

	recurse(item_group)
	return parents


def sync_collections_from_erp_item(product_id: int, erpnext_item):
	"""Sync collections from ERPNext item to Shopify, avoiding duplicates."""
	collections = []
	
	# Add item group and its parent groups
	if erpnext_item.item_group:
		collections.append(erpnext_item.item_group)
		parent_groups = get_all_parent_groups(erpnext_item.item_group)
		collections.extend(parent_groups)

	# Add gender if it exists
	gender = getattr(erpnext_item, "gender", None)
	if gender and gender.strip():
		collections.append(gender.strip())

	# Remove duplicates and empty values
	collections = list(set([col.strip() for col in collections if col and col.strip()]))
	
	if collections:
		create_shopify_log(message=f"Syncing collections for product {product_id}: {collections}")
		link_product_to_collections(product_id, collections)
	else:
		create_shopify_log(message=f"No collections to sync for product {product_id}")



@temp_shopify_session
def unlink_collections_from_product(product_id: int):
	try:
		collects = Collect.find(product_id=product_id)

		for collect in collects:
			try:
				collect.destroy()
			except Exception as e:
				frappe.log_error(f"Failed to delete collect {collect.id}: {e}", "Shopify Sync Error")

	except Exception as e:
		frappe.log_error(f"Failed to fetch collects for product {product_id}: {e}", "Shopify Sync Error")


def test_collection_functions():
	"""Test function to debug collection creation and linking issues."""
	try:
		create_shopify_log(message="Testing collection functions...")
		
		# Test 1: Try direct search for existing collection
		test_title = "Test Collection"
		existing = get_collection_by_title(test_title)
		if existing:
			create_shopify_log(message=f"Found existing collection: {existing.title} (ID: {existing.id})")
		else:
			create_shopify_log(message=f"No existing collection found for '{test_title}'")
		
		# Test 2: Try to create/find a test collection
		test_collection_id = get_or_create_shopify_collection(test_title)
		if test_collection_id:
			create_shopify_log(message=f"Successfully created/found test collection with ID: {test_collection_id}")
		else:
			create_shopify_log(message="Failed to create/find test collection", error=True)
		
		# Test 3: Test cache functionality
		create_shopify_log(message="Testing cache functionality...")
		cached_id = get_or_create_shopify_collection(test_title)
		if cached_id:
			create_shopify_log(message=f"Cache test successful - found collection ID: {cached_id}")
		else:
			create_shopify_log(message="Cache test failed", error=True)
			
	except Exception as e:
		create_shopify_log(message=f"Error in test_collection_functions: {str(e)}", error=True)




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
