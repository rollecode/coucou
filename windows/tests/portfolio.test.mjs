// The Portfolio pill's formatting (src/core/portfolio.ts): the Mac's digits, the sparkline and the accounts.

import { test } from "node:test";
import assert from "node:assert/strict";
import {
  EXCHANGES, WALLET_KINDS, accountValue, formatAmount, formatChange, formatValue, hasAnyAccount, readAccounts,
  sparklinePoints,
} from "../src/core/portfolio.ts";
import { pillDefinition } from "../src/core/pills.ts";

test("values keep the Mac's digits: none from $1000, two from $1, four below", () => {
  assert.equal(formatValue(12345.67), "$12,346");
  assert.equal(formatValue(42.5), "$42.5");
  assert.equal(formatValue(0.123456), "$0.1235");
  assert.equal(formatValue(0), "$0");
});

test("amounts and changes", () => {
  assert.equal(formatAmount(0), "0");
  assert.equal(formatAmount(2500.4), "2500");
  assert.equal(formatAmount(1.5), "1.5000");
  assert.equal(formatAmount(0.0001), "0.00010000");
  assert.equal(formatChange(2.345), "+2.3%");
  assert.equal(formatChange(-1), "-1.0%");
  assert.equal(formatChange(0), "+0.0%");
});

test("the sparkline spans the box, lowest price at the bottom", () => {
  assert.equal(sparklinePoints([1], 36, 14), "");
  assert.equal(sparklinePoints([1, 3, 2], 36, 14), "0,14 18,0 36,7");
  // A flat line sits on the bottom edge instead of dividing by zero.
  assert.equal(sparklinePoints([5, 5], 36, 14), "0,14 36,14");
});

test("accounts: none yet while loading, and their value", () => {
  assert.equal(readAccounts({}), null);
  const accounts = readAccounts({
    accounts: [{ id: "kraken", label: "Kraken", error: null, holdings: [
      { symbol: "BTC", amount: 0.5, price: 40000, change24h: 1, priceHistory: [] },
      { symbol: "XYZ", amount: 3, price: 0, change24h: null, priceHistory: [] },
    ] }],
  });
  assert.equal(accounts.length, 1);
  assert.equal(accountValue(accounts[0]), 20000);
  assert.equal(hasAnyAccount(null), false);
  assert.equal(hasAnyAccount({ exchanges: [], wallets: [] }), false);
  assert.equal(hasAnyAccount({ exchanges: ["okx"], wallets: [] }), true);
});

test("the Mac's exchanges and networks, in its order", () => {
  assert.deepEqual(EXCHANGES.map((e) => e.id), ["binance", "okx", "bybit", "kraken", "coinbase"]);
  assert.deepEqual(EXCHANGES.filter((e) => e.passphrase).map((e) => e.id), ["okx"]);
  assert.equal(WALLET_KINDS.length, 16);
  assert.equal(WALLET_KINDS.find((k) => k.id === "bnbChain").name, "BNB Chain");
  assert.deepEqual(pillDefinition("integration_portfolio").connect, { kind: "portfolio" });
});
