import frappe


def execute():
	"""F23: sites that never had the old Shopify connector were left with
	is_old_data_migrated = 0, so every save of Shopify Setting queued a
	migration job that stopped without finishing."""
	if frappe.db.exists("Custom Field", {"fieldname": "shopify_product_id"}) and frappe.db.exists(
		"Custom Field", {"fieldname": "shopify_variant_id"}
	):
		return  # old connector data may exist: let the real migration run

	frappe.db.set_value(
		"Shopify Setting", "Shopify Setting", "is_old_data_migrated", 1, update_modified=False
	)
	frappe.db.sql(
		"""update `tabEcommerce Integration Log`
		set status = 'Success', message = 'Nothing to migrate: the old Shopify connector was not used on this site.'
		where status = 'Queued' and method = 'ecommerce_integrations.shopify.utils.migrate_from_old_connector'"""
	)
