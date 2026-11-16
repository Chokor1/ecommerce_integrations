import frappe

SERVER_SCRIPT = "Update Shopping Selling Rate"
CLIENT_SCRIPT = "Item Price-Form"


def execute():
	"""Sites that priced Shopify through the "Update Shopping Selling Rate"
	server script (called from the Item Price form) move to the built-in
	price-list pricing, which also covers imports, deletes and dated prices.
	The two site scripts are switched off so nothing runs twice."""
	if not frappe.db.exists("Server Script", SERVER_SCRIPT):
		return

	setting = frappe.get_doc("Shopify Setting")
	if not setting.get("sync_price_from_price_list"):
		price_list = frappe.db.get_single_value("Selling Settings", "selling_price_list") or "Standard Selling"
		frappe.db.set_value("Shopify Setting", "Shopify Setting", {
			"sync_price_from_price_list": 1,
			"shopify_price_list": price_list,
			"price_tax_rate": 14,
		}, update_modified=False)

	frappe.db.set_value("Server Script", SERVER_SCRIPT, "disabled", 1, update_modified=False)

	# the client script only calls the server script; disable it only if that
	# is all its live (uncommented) code does
	script = frappe.db.get_value("Client Script", CLIENT_SCRIPT, "script")
	if script:
		live = "\n".join(l for l in script.splitlines() if l.strip() and not l.strip().startswith("//"))
		if "update_shopping_selling_rate" in live and live.count("frappe.call") == 1:
			frappe.db.set_value("Client Script", CLIENT_SCRIPT, "enabled", 0, update_modified=False)
