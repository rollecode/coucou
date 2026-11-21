// The Portfolio pill (src-tauri/src/portfolio.rs): what Settings offers to
// connect and how the card writes amounts. Port of the tables and formatters
// of PortfolioController.swift.

import { N_ } from "../i18n/i18n";

export const PORTFOLIO_ID = "integration_portfolio";

export type ExchangeId = "binance" | "okx" | "bybit" | "kraken" | "coinbase";

export type WalletKind =
  | "bitcoin" | "dogecoin" | "litecoin"
  | "ethereum" | "polygon" | "bnbChain" | "avalanche" | "arbitrum" | "optimism" | "base"
  | "solana" | "tron" | "xrp" | "cardano" | "cosmos" | "near";

export interface PortfolioWallet {
  kind: WalletKind;
  address: string;
  label: string;
}

/** What is connected: exchanges with keys stored, and wallets. Never a key. */
export interface PortfolioStatus {
  exchanges: ExchangeId[];
  wallets: PortfolioWallet[];
}

export interface PortfolioHolding {
  symbol: string;
  amount: number;
  /** USD; 0 when no price is known. */
  price: number;
  change24h: number | null;
  /** The last seven daily closes, oldest first. */
  priceHistory: number[];
}

export interface PortfolioAccount {
  id: string;
  label: string;
  holdings: PortfolioHolding[];
  error: string | null;
}

export interface ExchangeDef {
  id: ExchangeId;
  name: string;
  keyLabel: string;
  secretLabel: string;
  passphrase: boolean;
  instructions: string;
}

/** ExchangeKind on the Mac, in its order. Labels and instructions are English keys for `t()`. */
export const EXCHANGES: readonly ExchangeDef[] = [
  { id: "binance", name: "Binance", keyLabel: N_("API key"), secretLabel: N_("API secret"), passphrase: false,
    instructions: N_("In Binance → API Management, create a key with Read Info permission only. Do not enable Spot Trading or Withdrawals.") },
  { id: "okx", name: "OKX", keyLabel: N_("API key"), secretLabel: N_("API secret"), passphrase: true,
    instructions: N_("In OKX → API → Create V5 API Key. Set permission to Read only. Note your passphrase — it is shown only once.") },
  { id: "bybit", name: "Bybit", keyLabel: N_("API key"), secretLabel: N_("API secret"), passphrase: false,
    instructions: N_("In Bybit → API Management, create a key with Read Only permissions. Uncheck all other scopes.") },
  { id: "kraken", name: "Kraken", keyLabel: N_("API key"), secretLabel: N_("API secret"), passphrase: false,
    instructions: N_("In Kraken → Security → API, create a key with Query Funds permission only. Paste the Private Key (base64) as the secret.") },
  { id: "coinbase", name: "Coinbase", keyLabel: N_("Key name"), secretLabel: N_("Private key"), passphrase: false,
    instructions: N_("On coinbase.com → Settings → API (CDP) → Create API key, signature ECDSA, permission View only. Paste the key name (organizations/…/apiKeys/…) and the whole private key (BEGIN EC PRIVATE KEY…).") },
];

/** WalletKind on the Mac, in its order: name and address placeholder. */
export const WALLET_KINDS: readonly { id: WalletKind; name: string; placeholder: string }[] = [
  { id: "bitcoin", name: "Bitcoin", placeholder: "bc1q… / 1… / 3…" },
  { id: "dogecoin", name: "Dogecoin", placeholder: "D…" },
  { id: "litecoin", name: "Litecoin", placeholder: "ltc1… / L… / M…" },
  { id: "ethereum", name: "Ethereum", placeholder: "0x…" },
  { id: "polygon", name: "Polygon", placeholder: "0x…" },
  { id: "bnbChain", name: "BNB Chain", placeholder: "0x…" },
  { id: "avalanche", name: "Avalanche", placeholder: "0x…" },
  { id: "arbitrum", name: "Arbitrum", placeholder: "0x…" },
  { id: "optimism", name: "Optimism", placeholder: "0x…" },
  { id: "base", name: "Base", placeholder: "0x…" },
  { id: "solana", name: "Solana", placeholder: "base58" },
  { id: "tron", name: "TRON", placeholder: "T…" },
  { id: "xrp", name: "XRP Ledger", placeholder: "r…" },
  { id: "cardano", name: "Cardano", placeholder: "addr1…" },
  { id: "cosmos", name: "Cosmos", placeholder: "cosmos1…" },
  { id: "near", name: "NEAR", placeholder: "alice.near" },
];

export function hasAnyAccount(status: PortfolioStatus | null): boolean {
  return !!status && (status.exchanges.length > 0 || status.wallets.length > 0);
}

/** The card's accounts, or null while the first fetch has not answered. */
export function readAccounts(data: Record<string, unknown>): PortfolioAccount[] | null {
  return Array.isArray(data.accounts) ? (data.accounts as PortfolioAccount[]) : null;
}

export const holdingValue = (h: PortfolioHolding) => h.amount * h.price;

export function accountValue(a: PortfolioAccount): number {
  return a.holdings.reduce((sum, h) => sum + holdingValue(h), 0);
}

/** USD with the Mac's digits: none from $1000, two from $1, four below. */
export function formatValue(usd: number, locale = "en"): string {
  const digits = usd >= 1000 ? 0 : usd >= 1 ? 2 : 4;
  return new Intl.NumberFormat(locale, {
    style: "currency", currency: "USD", minimumFractionDigits: 0, maximumFractionDigits: digits,
  }).format(usd);
}

export function formatAmount(amount: number): string {
  if (amount === 0) return "0";
  if (amount >= 1000) return amount.toFixed(0);
  if (amount >= 1) return amount.toFixed(4);
  return amount.toFixed(8);
}

/** "+2.3%" / "-1.0%". */
export function formatChange(change: number): string {
  return `${change >= 0 ? "+" : ""}${change.toFixed(1)}%`;
}

/** The sparkline as SVG polyline points in a `width` × `height` box; "" under two prices. */
export function sparklinePoints(prices: number[], width: number, height: number): string {
  if (prices.length < 2) return "";
  const min = Math.min(...prices);
  const range = Math.max(...prices) - min || 1;
  return prices
    .map((p, i) => {
      const x = (width * i) / (prices.length - 1);
      const y = height - (height * (p - min)) / range;
      return `${+x.toFixed(2)},${+y.toFixed(2)}`;
    })
    .join(" ");
}
