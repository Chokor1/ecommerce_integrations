// Shopify Catalogue — ERPNext products side by side with Shopify.
// Loaded by page/shopify_catalogue/shopify_catalogue.js (thin loader).
frappe.provide("shopify");

(function () {
	const API = "ecommerce_integrations.shopify.catalogue.";
	const esc = (v) => frappe.utils.escape_html(v == null ? "" : String(v));
	const icon = (name, size = "sm") => frappe.utils.icon(name, size);
	const num = (v) => (Number(v) || 0).toLocaleString(undefined, { maximumFractionDigits: 0 });
	const store = {
		get(key, fallback) {
			try {
				const v = localStorage.getItem(`shopify_catalogue:${key}`);
				return v == null ? fallback : JSON.parse(v);
			} catch (e) {
				return fallback;
			}
		},
		set(key, value) {
			try {
				localStorage.setItem(`shopify_catalogue:${key}`, JSON.stringify(value));
			} catch (e) {
				/* private mode */
			}
		},
	};

	// Sidebar sections. `action` is the bulk action offered for the section.
	const GROUPS = [
		{
			key: "attention",
			label: __("Needs attention"),
			icon: "solid-warning",
			items: [
				{
					key: "not_in_shopify",
					label: __("In stock, not in Shopify"),
					icon: "upload",
					tone: "warn",
					actions: ["send", "prices"],
					desc: __("Products with stock in the Shopify stores that are not in Shopify yet."),
					empty: __("Every product with stock in the Shopify stores is in Shopify."),
				},
				{
					key: "partial",
					label: __("Partly in Shopify"),
					icon: "branch",
					tone: "warn",
					actions: ["send", "prices"],
					desc: __("In Shopify, but some variants are not. Send adds every missing variant that has a Shopify price, with or without stock."),
					empty: __("No product in Shopify is missing variants."),
				},
				{
					key: "missing_price",
					label: __("Missing Shopify price"),
					icon: "money-coins-1",
					tone: "bad",
					actions: ["prices"],
					desc: __("Variants without a Shopify price are never sent, so they can't go live at 0. Add the price to the price list, or use Update prices if it is already there."),
					empty: __("Every product in stock or in Shopify has a Shopify price."),
				},
				{
					key: "pending",
					label: __("Waiting to sync"),
					icon: "refresh",
					tone: "bad",
					actions: [],
					desc: __("Saves that could not reach Shopify. They are retried every 30 minutes."),
					empty: __("Nothing is waiting: every change has reached Shopify."),
				},
				{
					key: "disabled_live",
					label: __("Disabled here, live in Shopify"),
					icon: "restriction",
					tone: "bad",
					actions: [],
					desc: __("Disabled in ERPNext but still Active in Shopify. Saving the item again sets it to Draft."),
					empty: __("No disabled product is still live in Shopify."),
				},
			],
		},
		{
			key: "shopify",
			label: __("In Shopify"),
			icon: "website",
			items: [
				{
					key: "in_shopify",
					label: __("All in Shopify"),
					icon: "website",
					tone: "brand",
					actions: ["send", "publish", "prices"],
					desc: __("Every product linked to a Shopify product. Send adds variants that are still missing; Publish makes products visible."),
					empty: __("No product is linked to Shopify yet."),
				},
				{
					key: "not_published",
					label: __("Not published"),
					icon: "view",
					tone: "warn",
					actions: ["publish", "send", "prices"],
					desc: __("In Shopify as Draft or Archived, or Active but not on the online store."),
					empty: __("Every product in Shopify is active and published."),
				},
				{
					key: "no_stock",
					label: __("Active, no stock here"),
					icon: "stock",
					tone: "muted",
					actions: ["send", "prices"],
					desc: __("Active in Shopify with no stock in the Shopify stores."),
					empty: __("Every active product has stock in the Shopify stores."),
				},
			],
		},
		{
			key: "catalogue",
			label: __("Catalogue"),
			icon: "list",
			items: [
				{
					key: "all",
					label: __("All products"),
					icon: "list",
					tone: "muted",
					actions: ["send", "prices"],
					desc: __("Every template and simple item in ERPNext."),
					empty: __("No products match."),
				},
				{
					key: "only_shopify",
					label: __("Only in Shopify"),
					icon: "arrow-down-left",
					tone: "brand",
					actions: ["import"],
					desc: __("Shopify products that are not linked to any ERPNext item. Import creates the items here."),
					empty: __("Every Shopify product is linked to an ERPNext item."),
				},
			],
		},
	];
	const PAGE_LENGTHS = [20, 50, 100, 500];
	const BUCKETS = {};
	GROUPS.forEach((g) => g.items.forEach((b) => (BUCKETS[b.key] = b)));

	const ACTIONS = {
		send: { label: __("Send to Shopify"), icon: "upload", method: "send_to_shopify", key: "item_code" },
		publish: { label: __("Publish"), icon: "view", method: "publish", key: "product_id" },
		import: { label: __("Import"), icon: "arrow-down-left", method: "import_from_shopify", key: "product_id" },
		prices: { label: __("Update prices"), icon: "money-coins-1", method: "update_prices", key: "item_code" },
	};
	const JOB_VERBS = {
		send: __("Sending to Shopify"),
		publish: __("Publishing"),
		import: __("Importing from Shopify"),
		prices: __("Updating prices"),
	};

	shopify.Catalogue = class Catalogue {
		constructor(wrapper) {
			this.wrapper = $(wrapper);
			this.page = wrapper.page;
			this.bucket = store.get("bucket", "not_in_shopify");
			if (!BUCKETS[this.bucket]) this.bucket = "not_in_shopify";
			this.filters = store.get("filters", {}) || {};
			this.sort = store.get("sort", "stock_desc");
			if (!/_(asc|desc)$/.test(this.sort)) this.sort = "stock_desc";
			this.page_length = PAGE_LENGTHS.includes(store.get("page_length")) ? store.get("page_length") : 50;
			this.rows = [];
			this.selected = new Set();
			this.all_matching = false;
			this.closed_groups = new Set(store.get("closed_groups", []));
			this.make();
			this.load_meta().then(() => this.load());
		}

		// ------------------------------------------------------------ shell
		make() {
			// Full-screen app like Invenza: the desk header is hidden by a body class
			// that only holds on this route. v13 never calls on_page_hide and pushState
			// routing fires no hashchange, so follow the desk's own page-change event.
			const own_route = () =>
				document.body.classList.toggle("scat-page", (frappe.get_route_str() || "").startsWith("shopify-catalogue"));
			$(document).off("page-change.scat").on("page-change.scat", own_route);
			own_route();
			this.own_route = own_route;

			this.wrapper.children(".page-head, .page-body").hide();
			this.$root = $(`<div class="scat"></div>`).appendTo(this.wrapper);
			if (store.get("rail", false) || window.innerWidth < 992) this.$root.addClass("is-rail");

			this.$side = $(`
				<aside class="scat-side" aria-label="${__("Catalogue sections")}">
					<div class="scat-brand">
						<div class="scat-brand-mark" title="${__("Shopify Catalogue")}"><svg class="scat-mark" viewBox="0 0 24 24" aria-hidden="true">
								<path class="scat-mark-back" d="M10.4 2.2H5.2A2 2 0 0 0 3.2 4.2v5.2"/>
								<path d="M13.6 5H8.4a2 2 0 0 0-2 2v5.4c0 .53.21 1.04.59 1.41l5.6 5.6a2 2 0 0 0 2.82 0l5.4-5.4a2 2 0 0 0 0-2.82l-5.6-5.6A2 2 0 0 0 13.6 5z"/>
								<circle cx="10.6" cy="9.2" r="1.3"/>
							</svg></div>
						<div class="scat-brand-text">
							<div class="scat-brand-name"><span class="scat-word-a">Shopify</span><span class="scat-word-b">Catalogue</span></div>
							<div class="scat-brand-shop"></div>
						</div>
					</div>
					<nav class="scat-nav"></nav>
					<div class="scat-foot">
						<div class="scat-fresh">
							<div class="scat-fresh-text"></div>
							<button class="scat-btn is-sm is-icon is-quiet scat-refresh" title="${__("Refresh Shopify data")}"
								aria-label="${__("Refresh Shopify data")}">${icon("refresh")}</button>
						</div>
						<div class="scat-foot-row">
							<a class="scat-btn is-sm is-quiet" href="/app" title="${__("Back to ISOFT ERP")}"
								aria-label="${__("Back to ISOFT ERP")}">${icon("home")}<span class="scat-hide-rail">${__("Desk")}</span></a>
							<button class="scat-btn is-sm is-quiet scat-fullscreen" title="${__("Full screen")}"
								aria-label="${__("Full screen")}">${icon("expand")}</button>
							<button class="scat-btn is-sm is-quiet scat-rail-toggle" title="${__("Collapse sidebar")}"
								aria-label="${__("Collapse sidebar")}">${icon("sidebar-collapse")}<span class="scat-hide-rail">${__("Collapse")}</span></button>
						</div>
					</div>
				</aside>`).appendTo(this.$root);

			this.$main = $(`
				<main class="scat-main">
					<header class="scat-head">
						<div class="scat-head-text">
							<h1 class="scat-title"></h1>
							<p class="scat-desc"></p>
						</div>
						<div class="scat-head-actions">
							<button class="scat-btn scat-export">${icon("file")}${__("Export CSV")}</button>
							<a class="scat-btn scat-admin" target="_blank" rel="noopener">${icon("arrow-up-right")}${__("Shopify admin")}</a>
						</div>
					</header>
					<div class="scat-notices"></div>
					<div class="scat-tools"></div>
					<div class="scat-job-slot"></div>
					<section class="scat-card">
						<div class="scat-table-wrap"><table class="scat-table"><thead></thead><tbody></tbody></table></div>
						<div class="scat-foot-bar"></div>
					</section>
				</main>`).appendTo(this.$root);

			this.$selbar = $(`<div class="scat-selbar" hidden role="region" aria-label="${__("Selection")}"></div>`).appendTo(this.$root);
			this.$drawer = $(`<aside class="scat-drawer" aria-hidden="true"></aside>`).appendTo(this.$root);

			this.render_nav();
			this.bind();
		}

		bind() {
			const me = this;
			this.$side.on("click", ".scat-link", function () {
				me.set_bucket($(this).attr("data-bucket"));
			});
			this.$side.on("click", ".scat-group-head", function () {
				const $g = $(this).closest(".scat-group");
				if (me.$root.hasClass("is-rail")) {
					const open = !$g.hasClass("is-flyout");
					me.$side.find(".scat-group").removeClass("is-flyout");
					if (open) {
						$g.addClass("is-flyout");
						const rect = this.getBoundingClientRect();
						$g.find(".scat-group-kids").css({ top: rect.top, left: me.$side[0].getBoundingClientRect().right + 6 });
					}
					return;
				}
				$g.toggleClass("is-closed");
				const key = $g.attr("data-group");
				$g.hasClass("is-closed") ? me.closed_groups.add(key) : me.closed_groups.delete(key);
				store.set("closed_groups", [...me.closed_groups]);
			});
			this.$side.on("click", ".scat-rail-toggle", () => {
				this.$root.toggleClass("is-rail");
				this.$side.find(".scat-group").removeClass("is-flyout");
				store.set("rail", this.$root.hasClass("is-rail"));
			});
			this.$side.on("click", ".scat-refresh", () => this.refresh_snapshot());
			this.$side.on("click", ".scat-fullscreen", () => {
				if (document.fullscreenElement) document.exitFullscreen();
				else document.documentElement.requestFullscreen().catch(() => {});
			});
			$(document).off("fullscreenchange.scat").on("fullscreenchange.scat", () => {
				const on = !!document.fullscreenElement;
				this.$side.find(".scat-fullscreen").html(icon(on ? "collapse" : "expand"))
					.attr({ title: on ? __("Exit full screen") : __("Full screen"), "aria-label": on ? __("Exit full screen") : __("Full screen") });
			});
			$(document).on("click.scat", (e) => {
				if (!$(e.target).closest(".scat-group").length) this.$side.find(".scat-group").removeClass("is-flyout");
			});

			this.$main.on("click", ".scat-export", () => this.export_csv());
			this.$main.on("change", ".scat-has-price", (e) => this.set_filter("has_price", e.target.checked ? 1 : ""));
			// column sorting: a new column starts in its natural direction, a second click flips it
			this.$main.on("click", ".scat-sort", (e) => {
				const col = $(e.currentTarget).attr("data-sort");
				const [cur, dir] = this.sort.split(/_(?=asc$|desc$)/);
				const first = ["stock", "price", "variants"].includes(col) ? "desc" : "asc";
				this.sort = `${col}_${cur === col ? (dir === "asc" ? "desc" : "asc") : first}`;
				store.set("sort", this.sort);
				this.load();
			});

			this.$main.on("input", ".scat-input", frappe.utils.debounce((e) => {
				this.set_filter("search", e.target.value.trim());
			}, 350));
			this.$main.on("change", ".scat-select", (e) => {
				const key = $(e.target).attr("data-filter");
				this.set_filter(key, e.target.value);
			});
			this.$main.on("click", ".scat-clear", () => {
				this.filters = {};
				store.set("filters", this.filters);
				this.render_tools();
				this.load();
			});

			// table
			this.$main.on("click", ".scat-size", (e) => {
				const size = Number($(e.currentTarget).attr("data-size"));
				if (!PAGE_LENGTHS.includes(size) || size === this.page_length) return;
				this.page_length = size;
				store.set("page_length", size);
				this.load();
			});
			// load the next batch when the table is scrolled near its end
			this.$main.find(".scat-table-wrap").on("scroll", (e) => {
				const el = e.currentTarget;
				if (el.scrollTop + el.clientHeight >= el.scrollHeight - 240) this.load_more();
			});
			this.$main.on("click", ".scat-load-more", () => this.load_more());
			this.$main.on("click", "tbody tr[data-key]", function (e) {
				if ($(e.target).closest(".scat-check, .scat-row-act, a").length) return;
				me.open_drawer($(this).attr("data-key"));
			});
			this.$main.on("keydown", "tbody tr[data-key]", function (e) {
				if (e.key === "Enter") me.open_drawer($(this).attr("data-key"));
			});
			this.$main.on("change", "tbody .scat-check", function () {
				const key = $(this).closest("tr").attr("data-key");
				this.checked ? me.selected.add(key) : me.selected.delete(key);
				me.all_matching = false;
				$(this).closest("tr").toggleClass("is-selected", this.checked);
				me.render_selbar();
				me.sync_head_check();
			});
			this.$main.on("change", "thead .scat-check", function () {
				me.rows.forEach((r) => (this.checked ? me.selected.add(me.key_of(r)) : me.selected.delete(me.key_of(r))));
				me.all_matching = false;
				me.render_rows();
				me.render_selbar();
			});
			this.$main.on("click", ".scat-row-act", function (e) {
				e.stopPropagation();
				const key = $(this).closest("tr").attr("data-key");
				me.run_action($(this).attr("data-action"), [key]);
			});
			this.$main.on("click", ".scat-job-stop", () => frappe.call(API + "stop_job").then(() => this.poll_job()));
			this.$main.on("click", ".scat-job-close", () => {
				this.job_dismissed = this.job && this.job.started_at;
				this.render_job();
			});
			this.$main.on("click", ".scat-job-more", () => {
				this.job_open = !this.job_open;
				this.render_job();
			});

			// selection bar
			this.$selbar.on("click", ".scat-sel-all", () => {
				this.all_matching = true;
				this.render_selbar();
			});
			this.$selbar.on("click", ".scat-sel-clear", () => this.clear_selection());
			this.$selbar.on("click", ".scat-sel-act", (e) => {
				const keys = this.all_matching ? null : [...this.selected];
				this.run_action($(e.currentTarget).attr("data-action"), keys);
			});

			// drawer
			this.$drawer.on("click", ".scat-drawer-close", () => this.close_drawer());
			this.$drawer.on("click", ".scat-drawer-act", function () {
				me.run_action($(this).attr("data-action"), [$(this).attr("data-key")]);
			});
			$(document).on("keydown.scat", (e) => {
				if (!this.$root.is(":visible")) return;
				if (e.key === "Escape" && this.$drawer.hasClass("is-open")) this.close_drawer();
				if (e.key === "/" && !$(e.target).is("input, textarea, select")) {
					e.preventDefault();
					this.$main.find(".scat-input").trigger("focus");
				}
			});

			frappe.realtime.on("shopify_catalogue_job", (job) => {
				this.job = job;
				this.render_job();
				this.load();
			});
		}

		on_show() {
			this.own_route();
			if (this.meta) this.load();
		}

		// ------------------------------------------------------------ data
		load_meta() {
			return frappe.call(API + "get_meta").then(({ message }) => {
				this.meta = message;
				this.job = message.job;
				this.snapshot = message.snapshot;
				this.$side.find(".scat-brand-shop").text(message.shop || __("Shopify not set up"));
				const admin = message.shop_handle
					? `https://admin.shopify.com/store/${encodeURIComponent(message.shop_handle)}/products`
					: null;
				this.$main.find(".scat-admin").toggle(!!admin).attr("href", admin);
				this.render_tools();
				this.render_fresh();
				this.render_notices();
				this.render_job();
				if (this.job && this.job.status === "running") this.poll_job();
				if (this.snapshot.refreshing) this.poll_snapshot();
			});
		}

		args(start, with_counts) {
			return {
				bucket: this.bucket,
				filters: this.filters,
				start,
				sort: this.sort,
				page_length: this.page_length,
				with_counts: with_counts ? 1 : 0,
			};
		}

		// first batch (with section counts); the rest arrive through load_more()
		load() {
			const token = (this.token = Math.random());
			this.loading_more = false;
			this.render_head();
			this.render_skeleton();
			return frappe
				.call({ method: API + "get_catalogue", args: this.args(0, true) })
				.then(({ message }) => {
					if (token !== this.token) return; // a newer request is on its way
					this.rows = message.rows || [];
					this.total = message.total || 0;
					this.counts = message.counts || {};
					this.render_counts();
					this.render_rows();
					this.$main.find(".scat-table-wrap").scrollTop(0);
					this.render_pager();
					this.render_selbar();
					this.fill_view();
				});
		}

		has_more() {
			return this.rows.length < this.total;
		}

		load_more() {
			if (this.loading_more || !this.has_more() || !this.rows.length) return;
			const token = this.token;
			this.loading_more = true;
			this.render_pager();
			frappe
				.call({ method: API + "get_catalogue", args: this.args(this.rows.length, false) })
				.then(({ message }) => {
					if (token !== this.token) return; // the view changed meanwhile
					const more = message.rows || [];
					this.rows = this.rows.concat(more);
					if (!more.length) this.total = this.rows.length;
					this.append_rows(more);
					this.render_selbar();
				})
				.always(() => {
					if (token !== this.token) return;
					this.loading_more = false;
					this.render_pager();
					this.fill_view();
				});
		}

		// a small batch may not fill the table: keep loading until it scrolls
		fill_view() {
			const el = this.$main.find(".scat-table-wrap")[0];
			if (el && this.has_more() && el.scrollHeight <= el.clientHeight + 40) this.load_more();
		}

		set_bucket(bucket) {
			if (!BUCKETS[bucket]) return;
			this.bucket = bucket;
			store.set("bucket", bucket);
			this.clear_selection(true);
			this.close_drawer();
			this.$side.find(".scat-group").removeClass("is-flyout");
			this.render_nav_active();
			this.render_tools();
			this.load();
		}

		set_filter(key, value) {
			if (value) this.filters[key] = value;
			else delete this.filters[key];
			store.set("filters", this.filters);
			this.clear_selection(true);
			this.render_tools(key === "search");
			this.load();
		}

		key_of(row) {
			return this.bucket === "only_shopify" ? String(row.product_id) : row.item_code;
		}

		// ------------------------------------------------------------ render: sidebar
		render_nav() {
			const html = GROUPS.map(
				(g) => `
				<div class="scat-group ${this.closed_groups.has(g.key) ? "is-closed" : ""}" data-group="${g.key}">
					<button class="scat-group-head" title="${esc(g.label)}" aria-label="${esc(g.label)}">
						<span class="scat-group-icon">${icon(g.icon)}</span>
						<span class="scat-group-label">${esc(g.label)}</span>
						<span class="scat-caret">${icon("small-down", "xs")}</span>
					</button>
					<div class="scat-group-kids">
						${g.items
							.map(
								(b) => `
							<button class="scat-link tone-${b.tone}" data-bucket="${b.key}" title="${esc(b.label)}">
								<span class="scat-link-icon">${icon(b.icon)}</span>
								<span class="scat-link-label">${esc(b.label)}</span>
								<span class="scat-count is-zero" data-count="${b.key}">–</span>
							</button>`
							)
							.join("")}
					</div>
				</div>`
			).join("");
			this.$side.find(".scat-nav").html(html);
			this.render_nav_active();
		}

		render_nav_active() {
			this.$side.find(".scat-link").removeClass("is-active").removeAttr("aria-current");
			this.$side.find(`.scat-link[data-bucket="${this.bucket}"]`).addClass("is-active").attr("aria-current", "page");
		}

		render_counts() {
			Object.keys(BUCKETS).forEach((key) => {
				const n = this.counts[key] || 0;
				this.$side.find(`[data-count="${key}"]`).text(num(n)).toggleClass("is-zero", !n);
			});
		}

		render_fresh() {
			const s = this.snapshot || {};
			const $t = this.$side.find(".scat-fresh-text");
			const $btn = this.$side.find(".scat-refresh");
			$btn.toggleClass("is-busy", !!s.refreshing).prop("disabled", !!s.refreshing);
			if (s.refreshing) {
				$t.html(`<b>${__("Reading Shopify…")}</b>${__("{0} products so far", [num(s.products)])}`);
			} else if (s.refreshed_at) {
				$t.html(`<b>${__("Shopify data")}</b>${esc(frappe.datetime.prettyDate(s.refreshed_at))} · ${num(s.products)} ${__("products")}`);
			} else {
				$t.html(`<b>${__("No Shopify data yet")}</b>${__("Refresh to read it")}`);
			}
		}

		render_notices() {
			const notes = [];
			if (this.meta && !this.meta.enabled) {
				notes.push(__("The Shopify integration is disabled in Shopify Setting: you can look, but nothing can be sent."));
			}
			if (this.snapshot && !this.snapshot.refreshed_at && !this.snapshot.refreshing) {
				notes.push(__("Shopify's status and publication are not loaded yet. Use the refresh button at the bottom of the sidebar."));
			}
			this.$main.find(".scat-notices").html(
				notes.map((n) => `<div class="scat-notice">${icon("solid-warning")}<span>${esc(n)}</span></div>`).join("")
			);
		}

		// ------------------------------------------------------------ render: head + tools
		render_head() {
			const b = BUCKETS[this.bucket];
			this.$main.find(".scat-title").text(b.label);
			this.$main.find(".scat-desc").text(b.desc);
			document.title = `${b.label} · ${__("Shopify Catalogue")}`;
		}

		render_tools(keep_search_focus) {
			if (!this.meta) return;
			const only = this.bucket === "only_shopify";
			const f = this.filters;
			const select = (key, label, options, value_key, label_key) => {
				const opts = (options || [])
					.map((o) => {
						const value = value_key ? o[value_key] : o;
						const text = label_key ? o[label_key] : o;
						return `<option value="${esc(value)}" ${f[key] === value ? "selected" : ""}>${esc(text)}</option>`;
					})
					.join("");
				return `<select class="scat-select ${f[key] ? "is-set" : ""}" data-filter="${key}" aria-label="${esc(label)}">
					<option value="">${esc(label)}</option>${opts}</select>`;
			};
			const active = Object.keys(f).length;
			const html = `
				<label class="scat-search">${icon("search")}
					<input class="scat-input" type="search" value="${esc(f.search || "")}"
						placeholder="${esc(only ? __("Search title or SKU") : __("Search name or code"))}"
						aria-label="${__("Search")}">
				</label>
				${only ? "" : select("store", __("All Shopify stores"), this.meta.stores, "warehouse", "name")}
				${only ? "" : select("brand", __("Any brand"), this.meta.brands)}
				${only ? "" : select("item_group", __("Any group"), this.meta.item_groups)}
				${only || !this.meta.genders.length ? "" : select("gender", __("Any gender"), this.meta.genders)}
				${only ? "" : `<label class="scat-toggle ${f.has_price ? "is-set" : ""}">
					<input type="checkbox" class="scat-check scat-has-price" ${f.has_price ? "checked" : ""}>
					${__("Hide products without price")}</label>`}
				${active ? `<button class="scat-clear">${__("Clear filters")}</button>` : ""}`;
			const $tools = this.$main.find(".scat-tools");
			const caret = keep_search_focus ? $tools.find(".scat-input")[0]?.selectionStart : null;
			$tools.html(html);
			if (keep_search_focus) {
				const input = $tools.find(".scat-input")[0];
				input.focus();
				if (caret != null) input.setSelectionRange(caret, caret);
			}
		}

		// ------------------------------------------------------------ render: table
		columns() {
			if (this.bucket === "only_shopify") {
				return [
					{ label: __("Shopify product"), sort: "name" },
					{ label: __("Variants"), num: true, sort: "variants" },
					{ label: __("Shopify stock"), num: true, sort: "stock" },
					{ label: __("Status"), sort: "status" },
				];
			}
			const store = this.filters.store && (this.meta.stores.find((s) => s.warehouse === this.filters.store) || {}).name;
			return [
				{ label: __("Product"), sort: "name" },
				{ label: store ? __("Stock in {0}", [store]) : __("Stock here"), num: true, sort: "stock" },
				{ label: __("Variants in Shopify"), num: true, sort: "variants" },
				{ label: __("Shopify price"), num: true, sort: "price" },
				{ label: __("Shopify"), sort: "status" },
			];
		}

		render_skeleton() {
			const cols = this.columns();
			this.render_thead(cols);
			const cells = cols.map(() => `<td><span style="width:${40 + Math.round(Math.random() * 50)}%"></span></td>`).join("");
			this.$main.find("tbody").html(
				Array.from({ length: 8 }, () => `<tr class="scat-skel"><td class="scat-col-check"></td>${cells}<td></td></tr>`).join("")
			);
		}

		th(c) {
			if (!c.sort) return `<th class="${c.num ? "scat-num" : ""}">${esc(c.label)}</th>`;
			const [cur, dir] = this.sort.split(/_(?=asc$|desc$)/);
			const on = cur === c.sort;
			const aria = on ? (dir === "asc" ? "ascending" : "descending") : "none";
			return `<th class="${c.num ? "scat-num" : ""}" aria-sort="${aria}">
				<button class="scat-sort ${on ? "is-on" : ""}" data-sort="${c.sort}"
					title="${esc(__("Sort by {0}", [c.label]))}">${esc(c.label)}<span class="scat-sort-arrow">${icon(on ? (dir === "asc" ? "sort-ascending" : "sort-descending") : "sort", "xs")}</span></button></th>`;
		}

		render_thead(cols) {
			this.$main.find("thead").html(`<tr>
				<th class="scat-col-check"><input type="checkbox" class="scat-check" aria-label="${__("Select page")}"></th>
				${cols.map((c) => this.th(c)).join("")}
				<th class="scat-col-act"><span class="sr-only">${__("Actions")}</span></th></tr>`);
		}

		render_rows() {
			const cols = this.columns();
			this.render_thead(cols);
			const $body = this.$main.find("tbody");
			if (!this.rows.length) {
				const filtered = Object.keys(this.filters).length;
				$body.html(`<tr><td colspan="${cols.length + 2}"><div class="scat-empty">
					<div class="scat-empty-title">${filtered ? __("No products match these filters") : __("Nothing here")}</div>
					<p>${esc(filtered ? __("Try another brand, group or store, or clear the filters.") : BUCKETS[this.bucket].empty)}</p>
					${filtered ? `<button class="scat-btn scat-clear">${__("Clear filters")}</button>` : ""}
				</div></td></tr>`);
				this.sync_head_check();
				return;
			}
			$body.html(this.rows_html(this.rows));
			this.sync_head_check();
			if (this.open_key) $body.find(`tr[data-key="${CSS.escape(this.open_key)}"]`).addClass("is-open");
		}

		rows_html(rows) {
			return rows.map((r) => (this.bucket === "only_shopify" ? this.row_shopify(r) : this.row_erp(r))).join("");
		}

		append_rows(rows) {
			if (!rows.length) return;
			this.$main.find("tbody").append(this.rows_html(rows));
			this.sync_head_check();
		}

		thumb(src) {
			return `<div class="scat-thumb">${src ? `<img src="${esc(src)}" alt="" loading="lazy">` : icon("image")}</div>`;
		}

		status_pills(r) {
			const pills = [];
			if (!r.product_id) {
				pills.push(`<span class="scat-pill is-off">${__("Not in Shopify")}</span>`);
			} else if (!r.shopify_status) {
				pills.push(`<span class="scat-pill is-off">${__("In Shopify")}</span>`);
			} else if (r.shopify_status === "active") {
				pills.push(
					r.published
						? `<span class="scat-pill is-ok">${__("Active")}</span>`
						: `<span class="scat-pill is-warn">${__("Not published")}</span>`
				);
			} else {
				pills.push(`<span class="scat-pill is-off">${esc(frappe.utils.to_title_case(r.shopify_status))}</span>`);
			}
			if (r.pending) pills.push(`<span class="scat-pill is-bad">${__("Waiting to sync")}</span>`);
			if (r.disabled) pills.push(`<span class="scat-pill is-bad">${__("Disabled here")}</span>`);
			return `<div class="scat-pills">${pills.join("")}</div>`;
		}

		price(r) {
			if (!r.min_rate && !r.max_rate) return r.unpriced ? "" : `<span class="scat-muted">—</span>`;
			const f = (v) => format_currency(v, null, 0);
			return r.min_rate === r.max_rate ? f(r.max_rate) : `${f(r.min_rate)} – ${f(r.max_rate)}`;
		}

		row_erp(r) {
			const key = r.item_code;
			const sel = this.selected.has(key) || this.all_matching;
			const meta = [r.item_code, r.brand, r.item_group, r.gender].filter(Boolean).map(esc).join(" · ");
			const stores = (r.stores || []).map((s) => `${s.store}: ${num(s.qty)}`).join("\n");
			const variants = r.has_variants
				? `${num(r.linked_variants)} / ${num(r.variants)}`
				: r.product_id ? __("Yes") : `<span class="scat-muted">${__("No")}</span>`;
			const missing = r.product_id && r.unlinked
				? `<span class="scat-sub is-warn">${r.unlinked_in_stock
					? __("{0} missing · {1} in stock", [num(r.unlinked), num(r.unlinked_in_stock)])
					: __("{0} missing", [num(r.unlinked)])}</span>`
				: "";
			const unpriced = r.unpriced
				? `<span class="scat-sub is-bad">${r.has_variants ? __("{0} without price", [num(r.unpriced)]) : __("No price")}</span>`
				: "";
			// nothing can go without a Shopify price, so don't offer it
			const can_send = !r.disabled && r.missing_priced > 0;
			const admin = this.admin_url(r.product_id);
			return `<tr data-key="${esc(key)}" tabindex="0" class="${sel ? "is-selected" : ""}">
				<td class="scat-col-check"><input type="checkbox" class="scat-check" ${sel ? "checked" : ""} aria-label="${esc(__("Select {0}", [r.item_name]))}"></td>
				<td><div class="scat-prod">${this.thumb(r.image || r.shopify_image)}
					<div class="scat-prod-text"><div class="scat-prod-name">${esc(r.item_name || r.item_code)}</div>
					<div class="scat-prod-meta">${meta}</div></div></div></td>
				<td class="scat-num" title="${esc(stores)}"><span class="scat-stock ${r.stock > 0 ? "" : "is-zero"}">${num(r.stock)}</span></td>
				<td class="scat-num">${variants}${missing}</td>
				<td class="scat-num">${this.price(r)}${unpriced}</td>
				<td>${this.status_pills(r)}</td>
				<td class="scat-col-act">
					${can_send ? `<button class="scat-btn is-sm is-icon scat-row-act" data-action="send" title="${esc(r.product_id ? __("Send {0} missing variants to Shopify", [num(r.missing_priced)]) : __("Send to Shopify"))}" aria-label="${__("Send to Shopify")}">${icon("upload")}</button>` : ""}
					${admin ? `<a class="scat-btn is-sm is-icon" href="${admin}" target="_blank" rel="noopener" title="${__("Open in Shopify")}" aria-label="${__("Open in Shopify")}">${icon("arrow-up-right")}</a>` : ""}
				</td></tr>`;
		}

		row_shopify(r) {
			const key = String(r.product_id);
			const sel = this.selected.has(key) || this.all_matching;
			const skus = (r.skus || "").split(", ").filter(Boolean);
			const meta = [r.brand, r.item_group, skus.slice(0, 3).join(", ") + (skus.length > 3 ? "…" : "")].filter(Boolean).map(esc).join(" · ");
			const admin = this.admin_url(r.product_id);
			return `<tr data-key="${esc(key)}" tabindex="0" class="${sel ? "is-selected" : ""}">
				<td class="scat-col-check"><input type="checkbox" class="scat-check" ${sel ? "checked" : ""} aria-label="${esc(__("Select {0}", [r.item_name]))}"></td>
				<td><div class="scat-prod">${this.thumb(r.shopify_image)}
					<div class="scat-prod-text"><div class="scat-prod-name">${esc(r.item_name)}</div>
					<div class="scat-prod-meta">${meta || esc(key)}</div></div></div></td>
				<td class="scat-num">${num(r.variants)}</td>
				<td class="scat-num"><span class="scat-stock ${r.shopify_inventory > 0 ? "" : "is-zero"}">${num(r.shopify_inventory)}</span></td>
				<td>${this.status_pills(Object.assign({}, r, { product_id: key }))}</td>
				<td class="scat-col-act">
					<button class="scat-btn is-sm is-icon scat-row-act" data-action="import" title="${__("Import")}" aria-label="${__("Import")}">${icon("arrow-down-left")}</button>
					${admin ? `<a class="scat-btn is-sm is-icon" href="${admin}" target="_blank" rel="noopener" title="${__("Open in Shopify")}" aria-label="${__("Open in Shopify")}">${icon("arrow-up-right")}</a>` : ""}
				</td></tr>`;
		}

		admin_url(product_id) {
			if (!product_id || !this.meta || !this.meta.shop_handle) return null;
			return `https://admin.shopify.com/store/${encodeURIComponent(this.meta.shop_handle)}/products/${encodeURIComponent(product_id)}`;
		}

		render_pager() {
			const shown = Math.min(this.rows.length, this.total);
			let status = "";
			if (this.loading_more) status = `<span class="scat-loading">${icon("refresh")}${__("Loading more…")}</span>`;
			else if (this.has_more())
				status = `<button class="scat-clear scat-load-more">${__("Load {0} more", [num(Math.min(this.page_length, this.total - shown))])}</button>`;
			this.$main.find(".scat-foot-bar").html(`
				<span>${this.total ? __("{0}–{1} of {2}", [num(1), num(shown), num(this.total)]) : __("0 of 0")}</span>
				${status}
				<span class="scat-spacer"></span>
				<span class="scat-size-label" id="scat-size-label">${__("Load per scroll")}</span>
				<div class="scat-seg" role="group" aria-labelledby="scat-size-label">${PAGE_LENGTHS.map(
					(n) => `<button class="scat-size ${n === this.page_length ? "is-on" : ""}" data-size="${n}"
						aria-pressed="${n === this.page_length}">${n}</button>`
				).join("")}</div>`);
		}

		sync_head_check() {
			const $head = this.$main.find("thead .scat-check");
			const keys = this.rows.map((r) => this.key_of(r));
			const n = keys.filter((k) => this.selected.has(k)).length;
			$head.prop("checked", this.all_matching || (keys.length && n === keys.length));
			$head.prop("indeterminate", !this.all_matching && n > 0 && n < keys.length);
		}

		// ------------------------------------------------------------ selection + actions
		clear_selection(silent) {
			this.selected.clear();
			this.all_matching = false;
			if (!silent) this.render_rows();
			this.render_selbar();
		}

		render_selbar() {
			const price_sync = this.meta && this.meta.price_sync && this.meta.price_sync.enabled;
			const actions = (BUCKETS[this.bucket].actions || [])
				.filter((k) => k !== "prices" || price_sync)
				.map((k) => [k, ACTIONS[k]]);
			const n = this.all_matching ? this.total : this.selected.size;
			this.$root.toggleClass("has-selection", !!n);
			if (!n) {
				this.$selbar.attr("hidden", true).empty();
				return;
			}
			const page_full = !this.all_matching && this.rows.length && this.rows.every((r) => this.selected.has(this.key_of(r)));
			const offer_all = page_full && this.total > this.rows.length;
			this.$selbar.removeAttr("hidden").html(`
				<span class="scat-selbar-text">${this.all_matching ? __("All <b>{0}</b> in this view", [num(n)]) : __("<b>{0}</b> selected", [num(n)])}</span>
				${offer_all ? `<button class="scat-link-btn scat-sel-all">${__("Select all {0}", [num(this.total)])}</button>` : ""}
				${actions
					.map(([k, a], i) => `<button class="scat-btn ${i ? "" : "is-primary"} scat-sel-act" data-action="${k}">${icon(a.icon)}${esc(a.label)}</button>`)
					.join("")}
				<button class="scat-btn scat-sel-clear">${__("Clear")}</button>`);
		}

		run_action(kind, keys) {
			const action = ACTIONS[kind];
			if (!action) return;
			// prices are worked out and stored even with the integration off; they
			// reach Shopify once it is on
			if (kind !== "prices" && this.meta && !this.meta.enabled) {
				frappe.msgprint(__("The Shopify integration is disabled in Shopify Setting."));
				return;
			}
			if (this.job && this.job.status === "running") {
				frappe.msgprint(__("Another task is still running. Wait for it to finish or stop it."));
				return;
			}
			let args = {};
			let count = keys ? keys.length : this.total;
			if (keys) {
				if (action.key === "product_id" && this.bucket !== "only_shopify") {
					// rows are keyed by item; Publish works on Shopify products
					const ids = keys
						.map((k) => (this.rows.find((r) => r.item_code === k) || {}).product_id || (this.detail && this.detail.item.name === k && this.detail.product_id))
						.filter(Boolean);
					if (!ids.length) {
						frappe.msgprint(__("None of the selected products is in Shopify."));
						return;
					}
					args[`${action.key}s`] = JSON.stringify(ids);
					count = ids.length;
				} else {
					args[action.key === "item_code" ? "item_codes" : "product_ids"] = JSON.stringify(keys);
				}
			} else {
				args = { bucket: this.bucket, filters: JSON.stringify(this.filters) };
			}

			if (kind === "prices") {
				this.preview_prices(args);
				return;
			}
			const what = count === 1 ? __("1 product") : __("{0} products", [num(count)]);
			const messages = {
				send: __("Send {0} to Shopify?", [what]) + "<br><br>" +
					(this.meta.new_products_active
						? __("New products are created Active but not published to the online store; use Publish when they are ready.")
						: __("New products are created as Draft; use Publish when they are ready.")) +
					"<br>" + __("Variants without a Shopify price are left out."),
				publish: __("Set {0} Active and publish them to the online store?", [what]),
				import: __("Import {0} from Shopify into ERPNext as items?", [what]),
			};
			frappe.confirm(messages[kind], () => this.start_job(action, args));
		}

		start_job(action, args) {
			frappe.call({ method: API + action.method, args, freeze: true }).then(({ message }) => {
				this.job = message;
				this.job_dismissed = null;
				this.job_open = false;
				this.clear_selection();
				this.render_job();
				this.poll_job();
			});
		}

		preview_prices(args) {
			frappe
				.call({ method: API + "preview_prices", args, freeze: true, freeze_message: __("Working out the prices…") })
				.then(({ message: p }) => {
					const f = (v) => format_currency(v, null, 0);
					const rows = p.sample
						.map((r) => `<tr><td><div class="scat-vlabel">${esc(r.item_name || r.item_code)}</div><div class="scat-vcode">${esc(r.item_code)}${r.in_shopify ? "" : " · " + __("not in Shopify")}</div></td>
							<td class="scat-num">${r.old ? f(r.old) : `<span class="scat-muted">${__("none")}</span>`}</td>
							<td class="scat-num"><b>${f(r.new)}</b></td></tr>`)
						.join("");
					const body = `
						<div class="scat-dialog">
							<p>${__("Price list <b>{0}</b> valid today, plus each item's tax, rounded to whole units.", [esc(p.price_list)])}</p>
							<div class="scat-facts">
								<div class="scat-fact"><span>${__("Will change")}</span><b>${num(p.change)}</b></div>
								<div class="scat-fact"><span>${__("Of those, in Shopify")}</span><b>${num(p.change_in_shopify)}</b></div>
								<div class="scat-fact"><span>${__("Already right")}</span><b>${num(p.unchanged)}</b></div>
							</div>
							${p.no_price ? `<p class="scat-sub is-warn">${__("{0} items have no price valid today in the price list and are left as they are.", [num(p.no_price)])}</p>` : ""}
							${rows ? `<h4 class="scat-h4">${__("Biggest changes")}</h4>
								<table class="scat-vtable"><thead><tr><th>${__("Item")}</th><th class="scat-num">${__("Now")}</th><th class="scat-num">${__("New")}</th></tr></thead>
								<tbody>${rows}</tbody></table>` : ""}
						</div>`;
					const d = new frappe.ui.Dialog({
						title: __("Update prices from the price list"),
						size: "large",
						fields: [{ fieldtype: "HTML", fieldname: "body", options: body }],
						primary_action_label: p.change ? __("Update {0} prices", [num(p.change)]) : __("Close"),
						primary_action: () => {
							d.hide();
							if (p.change) this.start_job(ACTIONS.prices, args);
						},
					});
					d.show();
				});
		}

		// ------------------------------------------------------------ job strip
		poll_job() {
			clearTimeout(this.job_timer);
			frappe.call({ method: API + "get_status", args: {} }).then(({ message }) => {
				const was_running = this.job && this.job.status === "running";
				this.job = message.job;
				this.snapshot = message.snapshot;
				this.render_job();
				this.render_fresh();
				if (this.job && this.job.status === "running") {
					this.job_timer = setTimeout(() => this.poll_job(), 2500);
				} else if (was_running) {
					this.load();
				}
			});
		}

		render_job() {
			const job = this.job;
			const $slot = this.$main.find(".scat-job-slot");
			if (!job || (job.status !== "running" && this.job_dismissed === job.started_at)) {
				$slot.empty();
				return;
			}
			const running = job.status === "running";
			const pct = job.total ? Math.round((job.done / job.total) * 100) : 0;
			const parts = [__("{0} done", [num(job.ok)])];
			if (job.skipped) parts.push(__("{0} skipped", [num(job.skipped)]));
			if (job.failed) parts.push(__("{0} failed", [num(job.failed)]));
			const head = running
				? `<b>${esc(JOB_VERBS[job.kind])}</b> · ${__("{0} of {1}", [num(job.done), num(job.total)])}`
				: job.status === "finished"
				? `<b>${esc(JOB_VERBS[job.kind])}: ${__("finished")}</b>`
				: `<b>${esc(JOB_VERBS[job.kind])}: ${__("stopped after {0} of {1}", [num(job.done), num(job.total)])}</b>`;
			const problems = job.problems || [];
			$slot.html(`
				<div class="scat-job ${running ? "" : job.status === "finished" ? "is-done" : "is-stopped"}" role="status">
					<div class="scat-job-row">
						<div class="scat-job-text">${head} · ${parts.join(" · ")}
							${job.stopped_reason ? `<div class="scat-sub is-warn">${esc(job.stopped_reason)}</div>` : ""}
							${running && job.current ? `<div class="scat-sub">${esc(job.current)}</div>` : ""}</div>
						${problems.length ? `<button class="scat-btn is-sm is-quiet scat-job-more">${this.job_open ? __("Hide details") : __("Details ({0})", [num(problems.length)])}</button>` : ""}
						${running ? `<button class="scat-btn is-sm scat-job-stop">${__("Stop")}</button>`
							: `<button class="scat-btn is-sm is-icon is-quiet scat-job-close" aria-label="${__("Dismiss")}">${icon("close")}</button>`}
					</div>
					<div class="scat-job-bar"><span style="width:${pct}%"></span></div>
					${this.job_open && problems.length ? `<ul class="scat-job-problems">${problems
						.slice()
						.reverse()
						.map((p) => `<li><span class="scat-pcode">${esc(p.target)}</span><span class="${p.outcome === "failed" ? "scat-sub is-bad" : "scat-sub"}">${esc(p.note)}</span></li>`)
						.join("")}</ul>` : ""}
				</div>`);
		}

		// ------------------------------------------------------------ snapshot refresh
		refresh_snapshot() {
			frappe.call(API + "start_refresh").then(({ message }) => {
				this.snapshot = Object.assign({}, message, { refreshing: true });
				this.render_fresh();
				this.poll_snapshot();
			});
		}

		poll_snapshot() {
			clearTimeout(this.snap_timer);
			this.snap_timer = setTimeout(() => {
				frappe.call(API + "get_status").then(({ message }) => {
					this.snapshot = message.snapshot;
					this.render_fresh();
					if (this.snapshot.refreshing) this.poll_snapshot();
					else {
						this.render_notices();
						this.load();
					}
				});
			}, 3000);
		}

		// ------------------------------------------------------------ export
		export_csv() {
			const params = new URLSearchParams({ bucket: this.bucket, filters: JSON.stringify(this.filters) });
			window.open(`/api/method/${API}export_catalogue?${params.toString()}`);
		}

		// ------------------------------------------------------------ drawer
		open_drawer(key) {
			this.open_key = key;
			this.$main.find("tbody tr").removeClass("is-open");
			this.$main.find(`tbody tr[data-key="${CSS.escape(key)}"]`).addClass("is-open");
			this.$drawer.addClass("is-open").attr("aria-hidden", "false");

			if (this.bucket === "only_shopify") {
				const r = this.rows.find((x) => String(x.product_id) === key) || {};
				this.render_drawer_shopify(r);
				return;
			}
			this.$drawer.html(`<div class="scat-drawer-body"><div class="scat-skel"><span style="width:60%"></span></div></div>`);
			frappe.call({ method: API + "get_product_detail", args: { item_code: key } }).then(({ message }) => {
				if (this.open_key !== key) return;
				this.detail = message;
				this.render_drawer(message, this.rows.find((r) => r.item_code === key) || {});
			});
		}

		close_drawer() {
			this.open_key = null;
			this.$drawer.removeClass("is-open").attr("aria-hidden", "true");
			this.$main.find("tbody tr").removeClass("is-open");
		}

		drawer_head(src, title, sub) {
			return `<div class="scat-drawer-head">${this.thumb(src)}
				<div class="scat-drawer-title"><h3>${esc(title)}</h3><div>${sub}</div></div>
				<button class="scat-btn is-sm is-icon is-quiet scat-drawer-close" aria-label="${__("Close")}">${icon("close")}</button></div>`;
		}

		render_drawer(d, row) {
			const item = d.item;
			const sp = d.shopify;
			const variants = d.variants || [];
			const stock = variants.reduce((a, v) => a + (v.disabled ? 0 : v.stock), 0);
			const linked = variants.filter((v) => v.in_shopify).length;
			const stores = {};
			variants.forEach((v) => (v.stores || []).forEach((s) => (stores[s.store] = (stores[s.store] || 0) + (v.disabled ? 0 : s.qty))));
			const status = sp
				? `${frappe.utils.to_title_case(sp.status || "")}${sp.published ? "" : " · " + __("not published")}`
				: d.product_id ? __("In Shopify") : __("Not in Shopify");
			const admin = this.admin_url(d.product_id);
			const sendable = variants.filter((v) => !v.disabled && !v.in_shopify && v.price > 0).length;
			const can_send = !item.disabled && sendable > 0;
			const send_label = d.product_id ? __("Send {0} missing to Shopify", [num(sendable)]) : __("Send to Shopify");
			const can_publish = d.product_id && sp && (sp.status !== "active" || !sp.published) && !item.disabled;

			this.$drawer.html(`
				${this.drawer_head(item.image || (sp && sp.image), item.item_name, [esc(item.name), esc(item.brand), esc(item.item_group)].filter(Boolean).join(" · "))}
				<div class="scat-drawer-body">
					<div class="scat-facts">
						<div class="scat-fact"><span>${__("Stock here")}</span><b>${num(stock)}</b></div>
						<div class="scat-fact"><span>${__("Variants in Shopify")}</span><b>${num(linked)} / ${num(variants.length)}</b></div>
						<div class="scat-fact"><span>${__("Shopify")}</span><b title="${esc(status)}">${esc(status)}</b></div>
					</div>
					${Object.keys(stores).length ? `<div><h4 class="scat-h4">${__("Stock per Shopify store")}</h4><div class="scat-stores">${Object.entries(stores)
						.map(([s, q]) => `<span class="scat-store ${q > 0 ? "" : "is-zero"}">${esc(s)}<b>${num(q)}</b></span>`)
						.join("")}</div></div>` : ""}
					<div><h4 class="scat-h4">${item.has_variants ? __("Variants") : __("Item")}</h4>
						<table class="scat-vtable"><thead><tr>
							<th>${__("Variant")}</th><th class="scat-num">${__("Stock")}</th><th class="scat-num">${__("Price")}</th><th>${__("Shopify")}</th>
						</tr></thead><tbody>${variants
							.map((v) => `<tr class="${v.disabled ? "is-disabled" : ""}">
								<td><div class="scat-vlabel">${esc(v.label)}</div><div class="scat-vcode">${esc(v.item_code)}${v.disabled ? " · " + __("disabled") : ""}</div>
									${v.pending ? `<div class="scat-sub is-bad">${esc(v.pending)}</div>` : ""}</td>
								<td class="scat-num" title="${esc((v.stores || []).map((s) => `${s.store}: ${num(s.qty)}`).join("\n"))}">${num(v.stock)}</td>
								<td class="scat-num">${v.price > 0 ? format_currency(v.price, null, 0) : `<span class="scat-sub is-bad">${__("None")}</span>`}</td>
								<td>${v.in_shopify ? `<span class="scat-tick">${__("Yes")}</span>` : `<span class="scat-cross">${__("No")}</span>`}</td>
							</tr>`)
							.join("")}</tbody></table>
					</div>
				</div>
				<div class="scat-drawer-actions">
					${can_send ? `<button class="scat-btn is-primary scat-drawer-act" data-action="send" data-key="${esc(item.name)}">${icon("upload")}${esc(send_label)}</button>` : ""}
					${can_publish ? `<button class="scat-btn ${can_send ? "" : "is-primary"} scat-drawer-act" data-action="publish" data-key="${esc(item.name)}">${icon("view")}${__("Publish")}</button>` : ""}
					<a class="scat-btn" href="/app/item/${encodeURIComponent(item.name)}">${icon("edit")}${__("Open item")}</a>
					${admin ? `<a class="scat-btn" href="${admin}" target="_blank" rel="noopener">${icon("arrow-up-right")}${__("Open in Shopify")}</a>` : ""}
				</div>`);
		}

		render_drawer_shopify(r) {
			const key = String(r.product_id);
			const admin = this.admin_url(key);
			const skus = (r.skus || "").split(", ").filter(Boolean);
			this.$drawer.html(`
				${this.drawer_head(r.shopify_image, r.item_name, [esc(r.brand), esc(r.item_group), esc(key)].filter(Boolean).join(" · "))}
				<div class="scat-drawer-body">
					<div class="scat-facts">
						<div class="scat-fact"><span>${__("Variants")}</span><b>${num(r.variants)}</b></div>
						<div class="scat-fact"><span>${__("Shopify stock")}</span><b>${num(r.shopify_inventory)}</b></div>
						<div class="scat-fact"><span>${__("Status")}</span><b>${esc(frappe.utils.to_title_case(r.shopify_status || ""))}${r.published ? "" : " · " + __("not published")}</b></div>
					</div>
					<div><h4 class="scat-h4">${__("SKUs")}</h4><div class="scat-stores">${skus.length
						? skus.map((s) => `<span class="scat-store">${esc(s)}</span>`).join("")
						: `<span class="scat-muted">${__("No SKUs in Shopify")}</span>`}</div></div>
					<p class="scat-muted" style="margin:0">${__("Import creates ERPNext items for this product and links them, so stock and prices are sent from ERPNext afterwards.")}</p>
				</div>
				<div class="scat-drawer-actions">
					<button class="scat-btn is-primary scat-drawer-act" data-action="import" data-key="${esc(key)}">${icon("arrow-down-left")}${__("Import")}</button>
					${admin ? `<a class="scat-btn" href="${admin}" target="_blank" rel="noopener">${icon("arrow-up-right")}${__("Open in Shopify")}</a>` : ""}
				</div>`);
		}
	};
})();
