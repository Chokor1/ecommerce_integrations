import frappe
from frappe.utils.password import set_encrypted_password


def execute():
	"""Shopify Setting.shared_secret became a Password field (F18): move the
	plain-text value into the encrypted store, leave only a mask behind."""
	row = frappe.db.sql(
		"select value from tabSingles where doctype = 'Shopify Setting' and field = 'shared_secret'"
	)
	value = row[0][0] if row else None
	if not value or set(value) == {"*"}:
		return

	set_encrypted_password("Shopify Setting", "Shopify Setting", value, "shared_secret")
	frappe.db.sql(
		"update tabSingles set value = %s where doctype = 'Shopify Setting' and field = 'shared_secret'",
		("*" * len(value),),
	)
