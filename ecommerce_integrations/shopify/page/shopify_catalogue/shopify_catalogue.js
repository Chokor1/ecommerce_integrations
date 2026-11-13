// Thin loader. Frappe v13 keeps a desk page's script in localStorage with no
// version check, so the real code lives in public/ and is fetched with the
// file's own version every time the page opens.
frappe.pages["shopify-catalogue"].on_page_load = function (wrapper) {
	frappe.ui.make_app_page({ parent: wrapper, title: __("Shopify Catalogue"), single_column: true });

	frappe
		.call("ecommerce_integrations.shopify.catalogue.get_asset_version")
		.then(({ message: version }) => {
			const base = "/assets/ecommerce_integrations";
			$("<link>", { rel: "stylesheet", href: `${base}/css/shopify_catalogue.css?v=${version}` }).appendTo("head");
			const script = document.createElement("script");
			script.src = `${base}/js/shopify_catalogue.js?v=${version}`;
			script.onload = () => {
				wrapper.shopify_catalogue = new shopify.Catalogue(wrapper);
			};
			document.head.appendChild(script);
		});
};

frappe.pages["shopify-catalogue"].on_page_show = function (wrapper) {
	wrapper.shopify_catalogue && wrapper.shopify_catalogue.on_show();
};
