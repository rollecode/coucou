// Settings → Integrations → Portfolio — port of PortfolioSettingsSection
// (PortfolioSettingsView.swift): connected exchanges and wallets, each with a
// confirmed Remove, and the Add exchange / Add wallet forms that test before
// they save. The keys go straight to Rust and never come back.

import { Bridge } from "../core/bridge";
import {
  EXCHANGES, WALLET_KINDS, type PortfolioStatus, type WalletKind,
} from "../core/portfolio";
import { h, clear } from "../views/dom";
import { statusDot } from "./parts";
import { t } from "../i18n/i18n";

const ROWS = "display:flex;flex-direction:column;gap:6px";
const ERROR = "color:#f4505e";
const LABEL = "min-width:104px";

function shortAddress(address: string): string {
  return address.length > 16 ? `${address.slice(0, 8)}…${address.slice(-6)}` : address;
}

function input(type: "text" | "password", placeholder = ""): HTMLInputElement {
  return h("input", {
    type, placeholder, autocomplete: "off", spellcheck: "false", style: "flex:1 1 auto;min-width:0",
  }) as HTMLInputElement;
}

function field(label: string, el: HTMLElement): HTMLElement {
  return h("div", { class: "row" }, h("label", { style: LABEL, text: label }), el);
}

export function portfolioSettings(store: string): HTMLElement {
  const root = h("div", { style: ROWS });
  let status: PortfolioStatus = { exchanges: [], wallets: [] };

  /** An account row with a Remove that asks first, as the Mac's alert does. */
  function accountRow(name: string, detail: string, remove: () => Promise<PortfolioStatus>): HTMLElement {
    const row = h("div", { class: "row", style: "gap:8px" });
    const ask = () => {
      clear(row);
      const confirm = h("button", { class: "danger", text: t("Remove") });
      confirm.addEventListener("click", async () => {
        confirm.disabled = true;
        try {
          status = await remove();
        } catch {
          // Nothing removed: the list stays as it is.
        }
        draw();
      });
      row.append(
        h("span", { style: "font-size:12.5px", text: t("Remove {name}?", { name }) }),
        h("span", { class: "spacer" }),
        confirm,
        h("button", { text: t("Cancel"), onclick: draw }),
      );
    };
    row.append(
      statusDot(true),
      h("span", { style: "font-size:12.5px", text: name }),
      h("span", { class: "hint", text: detail }),
      h("span", { class: "spacer" }),
      h("button", { class: "danger", text: t("Remove"), onclick: ask }),
    );
    return row;
  }

  /** The two buttons and their footnote; `open` replaces them with a form. */
  function draw() {
    clear(root);
    for (const def of EXCHANGES.filter((e) => status.exchanges.includes(e.id))) {
      root.append(accountRow(def.name, t("read-only"), () => Bridge.portfolioRemoveExchange(def.id)));
    }
    for (const w of status.wallets) {
      const name = w.label || shortAddress(w.address);
      const kind = WALLET_KINDS.find((k) => k.id === w.kind)?.name ?? w.kind;
      root.append(accountRow(name, kind, () => Bridge.portfolioRemoveWallet(w.address)));
    }
    root.append(
      h("div", { class: "row", style: "gap:8px" },
        h("button", { text: t("Add exchange"), onclick: () => open(exchangeForm()) }),
        h("button", { text: t("Add wallet"), onclick: () => open(walletForm()) }),
      ),
      h("div", { class: "hint", text: t("Read-only access only. Keys stored in the {store}, never sent anywhere.", { store }) }),
    );
  }

  function open(form: HTMLElement) {
    clear(root);
    root.append(form);
  }

  /** Cancel, the primary button, and the error line above them. */
  function footer(primary: HTMLButtonElement, error: HTMLElement): HTMLElement[] {
    return [error, h("div", { class: "row", style: "gap:8px" }, h("button", { text: t("Cancel"), onclick: draw }), primary)];
  }

  function exchangeForm(): HTMLElement {
    const select = h("select", {}) as HTMLSelectElement;
    for (const def of EXCHANGES) select.append(h("option", { value: def.id, text: def.name }));
    const fields = h("div", { style: ROWS });
    const error = h("div", { class: "hint", style: ERROR });
    const save = h("button", { class: "primary", text: t("Test & Save") }) as HTMLButtonElement;
    let key = input("text");
    let secret = input("password");
    let passphrase = input("password");

    const def = () => EXCHANGES.find((e) => e.id === select.value) ?? EXCHANGES[0];
    const ready = () => {
      const d = def();
      save.disabled = !key.value.trim() || !secret.value.trim() || (d.passphrase && !passphrase.value.trim());
    };
    const redraw = () => {
      const d = def();
      key = input("text");
      secret = input("password");
      passphrase = input("password");
      for (const el of [key, secret, passphrase]) el.addEventListener("input", ready);
      error.textContent = "";
      clear(fields);
      fields.append(
        h("div", { class: "hint", text: t(d.instructions) }),
        field(t(d.keyLabel), key),
        field(t(d.secretLabel), secret),
      );
      if (d.passphrase) fields.append(field(t("Passphrase"), passphrase));
      ready();
    };
    select.addEventListener("change", redraw);
    save.addEventListener("click", async () => {
      save.disabled = true;
      save.textContent = t("Testing…");
      error.textContent = "";
      try {
        status = await Bridge.portfolioAddExchange(
          def().id, key.value.trim(), secret.value.trim(), passphrase.value.trim(),
        );
        draw();
      } catch (err) {
        error.textContent = String(err).replace(/^Error:\s*/, "");
        save.textContent = t("Test & Save");
        ready();
      }
    });
    redraw();
    return h("div", { style: ROWS },
      h("b", { style: "font-size:12.5px", text: t("Add exchange") }),
      field(t("Exchange"), select),
      fields,
      ...footer(save, error),
    );
  }

  function walletForm(): HTMLElement {
    const select = h("select", {}) as HTMLSelectElement;
    for (const k of WALLET_KINDS) select.append(h("option", { value: k.id, text: k.name }));
    const address = input("text", WALLET_KINDS[0].placeholder);
    const label = input("text", t("My wallet"));
    const error = h("div", { class: "hint", style: ERROR });
    const save = h("button", { class: "primary", text: t("Verify & Save") }) as HTMLButtonElement;
    save.disabled = true;
    let check = 0;

    // Rust holds the address rules; the newest answer wins.
    const ready = async () => {
      const mine = ++check;
      const ok = (await Bridge.portfolioAddressPlausible(select.value as WalletKind, address.value)) ?? false;
      if (mine === check) save.disabled = !ok;
    };
    address.addEventListener("input", () => void ready());
    select.addEventListener("change", () => {
      address.value = "";
      address.placeholder = WALLET_KINDS.find((k) => k.id === select.value)?.placeholder ?? "";
      error.textContent = "";
      void ready();
    });
    save.addEventListener("click", async () => {
      save.disabled = true;
      save.textContent = t("Testing…");
      error.textContent = "";
      try {
        status = await Bridge.portfolioAddWallet(select.value as WalletKind, address.value.trim(), label.value.trim());
        draw();
      } catch (err) {
        error.textContent = String(err).replace(/^Error:\s*/, "");
        save.textContent = t("Verify & Save");
        void ready();
      }
    });
    return h("div", { style: ROWS },
      h("b", { style: "font-size:12.5px", text: t("Add wallet") }),
      field(t("Network"), select),
      field(t("Address"), address),
      field(t("Label (optional)"), label),
      h("div", { class: "hint", text: t("Public address only. Coucou never touches your private keys.") }),
      ...footer(save, error),
    );
  }

  draw();
  void Bridge.portfolioStatus().then((fresh) => {
    if (!fresh) return;
    status = fresh;
    // A form the user already opened stays open.
    if (!root.querySelector("select")) draw();
  });
  return root;
}
