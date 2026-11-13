# Copyright (c) 2026, Frappe and contributors
# For license information, please see LICENSE

from frappe.model.document import Document


class ShopifyCatalogueProduct(Document):
	"""Read-only copy of one Shopify product, refreshed by
	ecommerce_integrations.shopify.catalogue.refresh_shopify_snapshot.
	The Shopify Catalogue page reads status and publication from here instead
	of calling Shopify for every page view."""
