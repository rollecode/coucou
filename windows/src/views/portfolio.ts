// The Portfolio card — DOM port of PortfolioCardView (PortfolioViews.swift):
// the total, then each account with its holdings by value, a 7-day sparkline
// and the 24 h change.

import { h, dot } from "./dom";
import { State, type AgentTask } from "../core/state";
import {
  PORTFOLIO_ID, accountValue, formatAmount, formatChange, formatValue, holdingValue, readAccounts,
  sparklinePoints, type PortfolioAccount, type PortfolioHolding,
} from "../core/portfolio";
import { language, t } from "../i18n/i18n";

const UP = "#4CAF50";
const DOWN = "#F4505E";
const SPARK_W = 36;
const SPARK_H = 14;

function sparkline(prices: number[], positive: boolean): SVGSVGElement {
  const ns = "http://www.w3.org/2000/svg";
  const el = document.createElementNS(ns, "svg");
  el.setAttribute("class", "pf-spark");
  el.setAttribute("width", String(SPARK_W));
  el.setAttribute("height", String(SPARK_H));
  el.setAttribute("viewBox", `0 0 ${SPARK_W} ${SPARK_H}`);
  el.setAttribute("aria-hidden", "true");
  const line = document.createElementNS(ns, "polyline");
  line.setAttribute("points", sparklinePoints(prices, SPARK_W, SPARK_H));
  line.setAttribute("fill", "none");
  line.setAttribute("stroke", positive ? UP : DOWN);
  line.setAttribute("stroke-width", "1.2");
  line.setAttribute("stroke-linecap", "round");
  line.setAttribute("stroke-linejoin", "round");
  el.append(line);
  return el;
}

function holdingRow(holding: PortfolioHolding): HTMLElement {
  const row = h("div", { class: "pf-row" },
    h("span", { class: "pf-symbol", text: holding.symbol }),
    h("span", { class: "pf-amount", text: formatAmount(holding.amount) }),
    h("span", { class: "pf-gap" }),
  );
  const change = holding.change24h;
  if (holding.priceHistory.length >= 2) row.append(sparkline(holding.priceHistory, change == null || change >= 0));
  if (change != null) {
    row.append(h("span", { class: "pf-change", style: `color:${change >= 0 ? UP : DOWN}`, text: formatChange(change) }));
  }
  if (holding.price > 0) row.append(h("span", { class: "pf-value", text: formatValue(holdingValue(holding), language()) }));
  return row;
}

function accountSection(account: PortfolioAccount): HTMLElement {
  const head = h("div", { class: "pf-acct" }, h("b", { text: account.label }));
  const value = accountValue(account);
  if (account.error) {
    head.append(h("span", { class: "pf-error", text: account.error, title: account.error }));
  } else if (value > 0) {
    head.append(h("span", { text: formatValue(value, language()) }));
  }
  const section = h("div", { class: "pf-account" }, head);
  if (!account.error) {
    const sorted = account.holdings.slice().sort((a, b) => holdingValue(b) - holdingValue(a));
    section.append(...sorted.map(holdingRow));
  }
  return section;
}

export function portfolioCard(task: AgentTask): HTMLElement {
  const accounts = readAccounts(State.integrations[PORTFOLIO_ID]?.data ?? {});
  const total = (accounts ?? []).reduce((sum, a) => sum + accountValue(a), 0);
  const head = h("div", { class: "int-head" }, dot(task.color, 7), h("b", { text: task.name }));
  if (accounts && total > 0) {
    head.append(h("span", { class: "int-total" }, h("span", { text: formatValue(total, language()) })));
  }
  const card = h("div", { class: "int-card pf-card" }, head);
  if (!accounts) {
    card.append(h("div", { class: "int-status", text: t("Loading…") }));
  } else if (accounts.length === 0) {
    card.append(h("div", { class: "int-status", text: t("No accounts connected — add one in Settings → Integrations.") }));
  } else {
    card.append(h("div", { class: "pf-list" }, ...accounts.map(accountSection)));
  }
  return card;
}
