// Replaced by the Shopify Catalogue page (import lives under "Only in Shopify").
frappe.pages["shopify-import-products"].on_page_load = function () {
	frappe.set_route("shopify-catalogue");
};
frappe.pages["shopify-import-products"].on_page_show = function () {
	frappe.set_route("shopify-catalogue");
};
