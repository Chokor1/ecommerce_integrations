import frappe

from ecommerce_integrations.ecommerce_integrations.doctype.ecommerce_integration_log.ecommerce_integration_log import (
	delete_logs_in_batches,
)


def execute():
	"""One-time purge of the step-by-step Shopify collection messages (F7).

	They were saved as status Queued with no method and are never processed.
	Real waiting jobs always carry a method, so they are kept."""
	if not frappe.db.table_exists("Ecommerce Integration Log"):
		return

	deleted = delete_logs_in_batches(
		"integration = 'shopify' and status = 'Queued' and ifnull(method, '') = ''", days=0
	)
	print(f"Deleted {deleted} Shopify info log rows")
