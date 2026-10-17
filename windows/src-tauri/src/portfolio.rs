// Portfolio pill — port of PortfolioController.swift. Balances of exchange
// accounts (read-only keys, in the OS keystore) and self-custody wallets
// (public addresses, in settings.json), valued in USD from Coinbase's public
// prices. Same endpoints, signatures and parsing as the Mac.
//
// integrations.rs runs the poller and sends the card its data; the commands
// below are Settings → Integrations → Portfolio.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ring::rand::{SecureRandom, SystemRandom};
use ring::signature::{EcdsaKeyPair, ECDSA_P256_SHA256_FIXED_SIGNING};
use ring::{digest, hmac};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tauri::{AppHandle, State};

use crate::i18n::t;
use crate::{net, secrets, settings, Shared};

pub const PILL_ID: &str = "integration_portfolio";

const TIMEOUT: Duration = Duration::from_secs(10);
const PRICE_TIMEOUT: Duration = Duration::from_secs(8);
/// Balances under this are dust and left out.
const DUST: f64 = 0.000001;
/// Valued at $1 when Coinbase has no price for them.
const STABLECOINS: [&str; 4] = ["USDT", "USDC", "BUSD", "DAI"];
/// Cash on an exchange: the day's exchange rate, no market data.
const FIAT: [&str; 7] = ["USD", "EUR", "GBP", "CHF", "CAD", "AUD", "JPY"];
/// Coinbase lists 250 accounts a page; ten pages is more than any account has.
const COINBASE_PAGES: usize = 10;
const COINBASE_HOST: &str = "api.coinbase.com";
const COINBASE_PATH: &str = "/api/v3/brokerage/accounts";

// ── Accounts ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Exchange {
    Binance,
    Okx,
    Bybit,
    Kraken,
    Coinbase,
}

impl Exchange {
    const ALL: [Exchange; 5] = [Self::Binance, Self::Okx, Self::Bybit, Self::Kraken, Self::Coinbase];

    fn id(self) -> &'static str {
        match self {
            Self::Binance => "binance",
            Self::Okx => "okx",
            Self::Bybit => "bybit",
            Self::Kraken => "kraken",
            Self::Coinbase => "coinbase",
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Binance => "Binance",
            Self::Okx => "OKX",
            Self::Bybit => "Bybit",
            Self::Kraken => "Kraken",
            Self::Coinbase => "Coinbase",
        }
    }

    fn needs_passphrase(self) -> bool {
        self == Self::Okx
    }

    /// The keystore entry for "key", "secret" or "passphrase", named as the Mac's Keychain items.
    fn entry(self, part: &str) -> String {
        format!("portfolio-{}-{part}", self.id())
    }

    fn stored(self) -> Option<Creds> {
        let creds = Creds {
            key: secrets::get_internal(&self.entry("key"))?,
            secret: secrets::get_internal(&self.entry("secret"))?,
            passphrase: secrets::get_internal(&self.entry("passphrase")).unwrap_or_default(),
        };
        creds.complete(self).then_some(creds)
    }
}

struct Creds {
    key: String,
    secret: String,
    passphrase: String,
}

impl Creds {
    fn complete(&self, exchange: Exchange) -> bool {
        !self.key.is_empty() && !self.secret.is_empty() && (!exchange.needs_passphrase() || !self.passphrase.is_empty())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum WalletKind {
    Bitcoin,
    Dogecoin,
    Litecoin,
    Ethereum,
    Polygon,
    BnbChain,
    Avalanche,
    Arbitrum,
    Optimism,
    Base,
    Solana,
    Tron,
    Xrp,
    Cardano,
    Cosmos,
    Near,
}

/// A self-custody wallet: a public address only. Kept in settings.json, like the Mac's UserDefaults.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Wallet {
    pub kind: WalletKind,
    pub address: String,
    pub label: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct Holding {
    symbol: String,
    amount: f64,
    price: f64,
    change24h: Option<f64>,
    price_history: Vec<f64>,
}

/// (symbol, amount) as an exchange reports them.
type Balances = Vec<(String, f64)>;

fn holdings(balances: Balances) -> Vec<Holding> {
    balances
        .into_iter()
        .filter(|(_, amount)| *amount > DUST)
        .map(|(symbol, amount)| Holding { symbol, amount, price: 0.0, change24h: None, price_history: Vec::new() })
        .collect()
}

// ── Fetching ──────────────────────────────────────────────────────────────────

enum Failure {
    Network,
    Status(u16, String),
    Parse,
}

impl Failure {
    fn exchange_message(self) -> String {
        match self {
            Self::Network => t("Network error. Check your connection."),
            Self::Status(code, detail) if detail.is_empty() => format!("HTTP {code}"),
            Self::Status(_, detail) => detail,
            Self::Parse => t("Unexpected response from the exchange."),
        }
    }

    fn wallet_message(self) -> String {
        match self {
            Self::Network | Self::Status(..) => t("Network error. Check your connection."),
            Self::Parse => t("Unexpected response from the exchange."),
        }
    }
}

fn client(timeout: Duration) -> reqwest::Client {
    reqwest::Client::builder().timeout(timeout).build().unwrap_or_default()
}

async fn send_json(request: reqwest::RequestBuilder) -> Result<Value, Failure> {
    let response = request.send().await.map_err(|_| Failure::Network)?;
    let status = response.status();
    let body = net::read_capped(response, net::MAX_BODY).await.map_err(|_| Failure::Network)?;
    if !status.is_success() {
        return Err(Failure::Status(status.as_u16(), net::error_detail(&body)));
    }
    serde_json::from_slice(&body).map_err(|_| Failure::Parse)
}

/// A number the APIs send as a JSON number or as a string.
fn num(v: &Value) -> Option<f64> {
    v.as_f64().or_else(|| v.as_str()?.trim().parse().ok())
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn hmac_sign(algorithm: hmac::Algorithm, key: &[u8], message: &[u8]) -> Vec<u8> {
    hmac::sign(&hmac::Key::new(algorithm, key), message).as_ref().to_vec()
}

fn base64url(bytes: &[u8]) -> String {
    crate::claude::base64_for(bytes).replace('+', "-").replace('/', "_").replace('=', "")
}

/// `ms` as ISO 8601 in UTC with milliseconds (OKX's timestamp header).
fn iso8601_utc(ms: u64) -> String {
    let secs = ms / 1000;
    // Days to a civil date (Howard Hinnant's algorithm).
    let z = (secs / 86_400) as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    let rem = secs % 86_400;
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:03}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60,
        ms % 1000
    )
}

async fn fetch_exchange(exchange: Exchange, creds: &Creds) -> Result<Vec<Holding>, String> {
    let http = client(TIMEOUT);
    let balances = match exchange {
        Exchange::Binance => {
            let query = format!("timestamp={}", now_ms());
            let sig = hex(&hmac_sign(hmac::HMAC_SHA256, creds.secret.as_bytes(), query.as_bytes()));
            let request = http
                .get(format!("https://api.binance.com/api/v3/account?{query}&signature={sig}"))
                .header("X-MBX-APIKEY", &creds.key);
            parse_binance(&send_json(request).await.map_err(Failure::exchange_message)?)
        }
        Exchange::Okx => {
            let ts = iso8601_utc(now_ms());
            let path = "/api/v5/account/balance";
            let sig = crate::claude::base64_for(&hmac_sign(
                hmac::HMAC_SHA256,
                creds.secret.as_bytes(),
                format!("{ts}GET{path}").as_bytes(),
            ));
            let request = http
                .get(format!("https://www.okx.com{path}"))
                .header("Content-Type", "application/json")
                .header("OK-ACCESS-KEY", &creds.key)
                .header("OK-ACCESS-SIGN", sig)
                .header("OK-ACCESS-TIMESTAMP", ts)
                .header("OK-ACCESS-PASSPHRASE", &creds.passphrase);
            parse_okx(&send_json(request).await.map_err(Failure::exchange_message)?)
        }
        Exchange::Bybit => {
            let ts = now_ms().to_string();
            let recv_window = "5000";
            let query = "accountType=UNIFIED";
            let message = format!("{ts}{}{recv_window}{query}", creds.key);
            let sig = hex(&hmac_sign(hmac::HMAC_SHA256, creds.secret.as_bytes(), message.as_bytes()));
            let request = http
                .get(format!("https://api.bybit.com/v5/account/wallet-balance?{query}"))
                .header("X-BAPI-API-KEY", &creds.key)
                .header("X-BAPI-SIGN", sig)
                .header("X-BAPI-TIMESTAMP", ts)
                .header("X-BAPI-RECV-WINDOW", recv_window);
            parse_bybit(&send_json(request).await.map_err(Failure::exchange_message)?)
        }
        Exchange::Kraken => {
            let secret = crate::recap::decode_base64(creds.secret.trim())
                .ok_or_else(|| t("No credentials found. Add them in Settings."))?;
            let nonce = now_ms().to_string();
            let path = "/0/private/Balance";
            let body = format!("nonce={nonce}");
            // API-Sign: HMAC-SHA512(path + SHA256(nonce + body)), keyed with the decoded secret.
            let mut message = path.as_bytes().to_vec();
            message.extend_from_slice(digest::digest(&digest::SHA256, format!("{nonce}{body}").as_bytes()).as_ref());
            let sig = crate::claude::base64_for(&hmac_sign(hmac::HMAC_SHA512, &secret, &message));
            let request = http
                .post(format!("https://api.kraken.com{path}"))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .header("API-Key", &creds.key)
                .header("API-Sign", sig)
                .body(body);
            parse_kraken(&send_json(request).await.map_err(Failure::exchange_message)?)?
        }
        Exchange::Coinbase => fetch_coinbase(&http, creds).await?,
    };
    balances.map(holdings).ok_or_else(|| Failure::Parse.exchange_message())
}

/// Advanced Trade accounts with a CDP API key: a fresh ES256 JWT per request.
async fn fetch_coinbase(http: &reqwest::Client, creds: &Creds) -> Result<Option<Balances>, String> {
    let key_name = creds.key.trim();
    let signer = coinbase_signer(&creds.secret).ok_or_else(|| {
        t("This private key can't be read. Paste the whole key, from BEGIN EC PRIVATE KEY to END EC PRIVATE KEY (ECDSA key).")
    })?;
    let mut totals: HashMap<String, f64> = HashMap::new();
    let mut cursor: Option<String> = None;
    for _ in 0..COINBASE_PAGES {
        let mut query = vec![("limit", "250".to_string())];
        if let Some(c) = &cursor {
            query.push(("cursor", c.clone()));
        }
        let jwt = coinbase_jwt(key_name, &signer, "GET", COINBASE_HOST, COINBASE_PATH, now_ms() / 1000)
            .ok_or_else(|| Failure::Parse.exchange_message())?;
        let request = http
            .get(format!("https://{COINBASE_HOST}{COINBASE_PATH}"))
            .query(&query)
            .header("Authorization", format!("Bearer {jwt}"))
            .header("Accept", "application/json");
        let page = send_json(request).await.map_err(Failure::exchange_message)?;
        let Some((balances, next)) = parse_coinbase_page(&page) else { return Ok(None) };
        for (currency, amount) in balances {
            *totals.entry(currency).or_default() += amount;
        }
        match next {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    Ok(Some(totals.into_iter().collect()))
}

/// The EC key as pasted: a PEM ("EC PRIVATE KEY" or "PRIVATE KEY"), its lines
/// joined by a one-line field, or with the "\n" escapes of cdp_api_key.json.
fn coinbase_signer(raw: &str) -> Option<EcdsaKeyPair> {
    let text = raw.replace("\\n", "\n");
    let mut body = text.trim_matches(|c: char| matches!(c, '"' | ' ' | '\n' | '\r' | '\t')).to_string();
    for marker in [
        "-----BEGIN EC PRIVATE KEY-----",
        "-----END EC PRIVATE KEY-----",
        "-----BEGIN PRIVATE KEY-----",
        "-----END PRIVATE KEY-----",
    ] {
        body = body.replace(marker, "");
    }
    body.retain(|c| !c.is_whitespace());
    let der = crate::recap::decode_base64(&body)?;
    let rng = SystemRandom::new();
    EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &der, &rng)
        .or_else(|_| EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &sec1_to_pkcs8(&der), &rng))
        .ok()
}

fn der(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    let len = body.len();
    match len {
        0..=0x7f => out.push(len as u8),
        0x80..=0xff => out.extend([0x81, len as u8]),
        _ => out.extend([0x82, (len >> 8) as u8, len as u8]),
    }
    out.extend_from_slice(body);
    out
}

/// A SEC1 "EC PRIVATE KEY" wrapped as PKCS#8, the form ring reads.
fn sec1_to_pkcs8(sec1: &[u8]) -> Vec<u8> {
    // INTEGER 0, then AlgorithmIdentifier { id-ecPublicKey, prime256v1 }.
    const HEAD: &[u8] = &[
        0x02, 0x01, 0x00, 0x30, 0x13, 0x06, 0x07, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x02, 0x01, 0x06, 0x08, 0x2a, 0x86,
        0x48, 0xce, 0x3d, 0x03, 0x01, 0x07,
    ];
    let mut body = HEAD.to_vec();
    body.extend(der(0x04, sec1));
    der(0x30, &body)
}

/// The JWT Coinbase documents for CDP keys: ES256, `kid` and a random `nonce`
/// in the header; `sub`, `iss: cdp`, `nbf`, `exp` (+120 s) and `uri` as claims.
fn coinbase_jwt(key_name: &str, signer: &EcdsaKeyPair, method: &str, host: &str, path: &str, now: u64) -> Option<String> {
    let rng = SystemRandom::new();
    let mut nonce = [0u8; 16];
    rng.fill(&mut nonce).ok()?;
    let header = json!({ "alg": "ES256", "kid": key_name, "nonce": hex(&nonce), "typ": "JWT" });
    let claims = json!({
        "sub": key_name, "iss": "cdp", "nbf": now, "exp": now + 120, "uri": format!("{method} {host}{path}"),
    });
    let input = format!("{}.{}", base64url(header.to_string().as_bytes()), base64url(claims.to_string().as_bytes()));
    let signature = signer.sign(&rng, input.as_bytes()).ok()?;
    Some(format!("{input}.{}", base64url(signature.as_ref())))
}

async fn fetch_wallet(wallet: &Wallet) -> Result<Vec<Holding>, String> {
    let address = wallet.address.as_str();
    if !is_plausible(address, wallet.kind) {
        return Err(t("Invalid address format."));
    }
    let http = client(TIMEOUT);
    let get = |url: String| http.get(url).header("Accept", "application/json");
    let rpc = |url: &str, body: Value| http.post(url).json(&body);
    let balance = match wallet.kind {
        WalletKind::Bitcoin => parse_btc(
            &send_json(get(format!("https://blockchain.info/balance?active={address}&cors=true"))).await,
            address,
        ),
        WalletKind::Dogecoin => parse_blockchair(&send_json(get(blockchair_url("dogecoin", address))).await, address),
        WalletKind::Litecoin => parse_blockchair(&send_json(get(blockchair_url("litecoin", address))).await, address),
        WalletKind::Solana => parse_sol(
            &send_json(rpc(
                "https://api.mainnet-beta.solana.com",
                json!({ "jsonrpc": "2.0", "id": 1, "method": "getBalance", "params": [address] }),
            ))
            .await,
        ),
        WalletKind::Tron => parse_tron(&send_json(get(format!("https://api.trongrid.io/v1/accounts/{address}"))).await),
        WalletKind::Xrp => parse_xrp(
            &send_json(rpc(
                "https://xrplcluster.com",
                json!({ "method": "account_info", "params": [{ "account": address, "ledger_index": "current" }] }),
            ))
            .await,
        ),
        WalletKind::Cardano => parse_cardano(
            &send_json(
                rpc("https://api.koios.rest/api/v1/address_info", json!({ "_addresses": [address] }))
                    .header("Accept", "application/json"),
            )
            .await,
        ),
        WalletKind::Cosmos => parse_cosmos(
            &send_json(get(format!(
                "https://rest.cosmos.directory/cosmoshub/cosmos/bank/v1beta1/balances/{address}"
            )))
            .await,
        ),
        WalletKind::Near => parse_near(
            &send_json(rpc(
                "https://rpc.mainnet.near.org",
                json!({
                    "jsonrpc": "2.0", "id": "dontcare", "method": "query",
                    "params": { "request_type": "view_account", "finality": "final", "account_id": address },
                }),
            ))
            .await,
        ),
        evm => {
            // Two public RPCs per chain: the second answers when the first is down or rate-limited.
            let body = json!({ "jsonrpc": "2.0", "id": 1, "method": "eth_getBalance", "params": [address, "latest"] });
            let mut last = Err(Failure::Network);
            for url in evm_rpcs(evm) {
                last = parse_evm(&send_json(rpc(url, body.clone())).await);
                if last.is_ok() {
                    break;
                }
            }
            last
        }
    };
    let amount = balance.map_err(Failure::wallet_message)?;
    // Cosmos may hold no ATOM at all: no holding, as on the Mac.
    Ok(amount
        .map(|a| vec![Holding { symbol: wallet_symbol(wallet.kind).into(), amount: a, price: 0.0, change24h: None, price_history: Vec::new() }])
        .unwrap_or_default())
}

fn blockchair_url(chain: &str, address: &str) -> String {
    format!("https://api.blockchair.com/{chain}/dashboards/address/{address}")
}

fn evm_rpcs(kind: WalletKind) -> [&'static str; 2] {
    match kind {
        WalletKind::Polygon => ["https://polygon-bor-rpc.publicnode.com", "https://polygon-rpc.com"],
        WalletKind::BnbChain => ["https://bsc-dataseed.binance.org", "https://bsc-rpc.publicnode.com"],
        WalletKind::Avalanche => ["https://api.avax.network/ext/bc/C/rpc", "https://avalanche-c-chain-rpc.publicnode.com"],
        WalletKind::Arbitrum => ["https://arb1.arbitrum.io/rpc", "https://arbitrum-one-rpc.publicnode.com"],
        WalletKind::Optimism => ["https://mainnet.optimism.io", "https://optimism-rpc.publicnode.com"],
        WalletKind::Base => ["https://mainnet.base.org", "https://base-rpc.publicnode.com"],
        _ => ["https://ethereum-rpc.publicnode.com", "https://cloudflare-eth.com"],
    }
}

fn wallet_symbol(kind: WalletKind) -> &'static str {
    match kind {
        WalletKind::Bitcoin => "BTC",
        WalletKind::Dogecoin => "DOGE",
        WalletKind::Litecoin => "LTC",
        WalletKind::Ethereum | WalletKind::Arbitrum | WalletKind::Optimism | WalletKind::Base => "ETH",
        WalletKind::Polygon => "POL",
        WalletKind::BnbChain => "BNB",
        WalletKind::Avalanche => "AVAX",
        WalletKind::Solana => "SOL",
        WalletKind::Tron => "TRX",
        WalletKind::Xrp => "XRP",
        WalletKind::Cardano => "ADA",
        WalletKind::Cosmos => "ATOM",
        WalletKind::Near => "NEAR",
    }
}

/// A quick local check before the network: the Mac's rules, plus only the
/// characters an address can hold, since it goes into URLs.
fn is_plausible(address: &str, kind: WalletKind) -> bool {
    let a = address.trim();
    if !a.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-')) {
        return false;
    }
    let len = a.len();
    let lower = a.to_ascii_lowercase();
    match kind {
        WalletKind::Bitcoin if lower.starts_with("bc1") => (14..=90).contains(&len),
        WalletKind::Bitcoin => (a.starts_with('1') || a.starts_with('3')) && (25..=34).contains(&len),
        WalletKind::Dogecoin => (a.starts_with('D') || a.starts_with('A')) && (25..=34).contains(&len),
        WalletKind::Litecoin if lower.starts_with("ltc1") => (26..=90).contains(&len),
        WalletKind::Litecoin => (a.starts_with('L') || a.starts_with('M')) && (25..=34).contains(&len),
        WalletKind::Solana => (32..=44).contains(&len),
        WalletKind::Tron => a.starts_with('T') && len == 34,
        WalletKind::Xrp => a.starts_with('r') && (25..=35).contains(&len),
        WalletKind::Cardano => lower.starts_with("addr1") && len >= 50,
        WalletKind::Cosmos => a.starts_with("cosmos1") && len == 45,
        WalletKind::Near if len == 64 => a.chars().all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c)),
        WalletKind::Near => (5..=64).contains(&len) && a.contains('.'),
        _ => lower.starts_with("0x") && len == 42 && a[2..].chars().all(|c| c.is_ascii_hexdigit()),
    }
}

// ── Parsing ───────────────────────────────────────────────────────────────────
// Exchange parsers give (symbol, amount) pairs, None when the answer is not the
// expected shape. Wallet parsers give the balance in whole coins.

fn parse_binance(json: &Value) -> Option<Balances> {
    let balances = json.get("balances")?.as_array()?;
    Some(
        balances
            .iter()
            .filter_map(|b| {
                let free = b.get("free")?.as_str()?.parse::<f64>().ok()?;
                let locked = b.get("locked")?.as_str()?.parse::<f64>().ok()?;
                Some((b.get("asset")?.as_str()?.to_string(), free + locked))
            })
            .collect(),
    )
}

fn parse_okx(json: &Value) -> Option<Balances> {
    let details = json.get("data")?.as_array()?.first()?.get("details")?.as_array()?;
    Some(
        details
            .iter()
            .filter_map(|d| {
                let avail = d.get("availBal")?.as_str()?.parse::<f64>().ok()?;
                let frozen = d.get("frozenBal")?.as_str()?.parse::<f64>().ok()?;
                Some((d.get("ccy")?.as_str()?.to_string(), avail + frozen))
            })
            .collect(),
    )
}

fn parse_bybit(json: &Value) -> Option<Balances> {
    let coins = json.pointer("/result/list/0/coin")?.as_array()?;
    Some(
        coins
            .iter()
            .filter_map(|c| {
                let balance = c.get("walletBalance")?.as_str()?.parse::<f64>().ok()?;
                Some((c.get("coin")?.as_str()?.to_string(), balance))
            })
            .collect(),
    )
}

/// Kraken reports its own errors in a 200: the first one is the message.
fn parse_kraken(json: &Value) -> Result<Option<Balances>, String> {
    if let Some(first) = json.get("error").and_then(Value::as_array).and_then(|e| e.first()) {
        return Err(first.as_str().map(str::to_string).unwrap_or_else(|| first.to_string()));
    }
    let Some(result) = json.get("result").and_then(Value::as_object) else { return Ok(None) };
    Ok(Some(result.iter().filter_map(|(raw, v)| Some((kraken_symbol(raw), num(v)?))).collect()))
}

/// Kraken's X/Z-prefixed names: XXBT is BTC, ZUSD is USD.
fn kraken_symbol(raw: &str) -> String {
    let known = match raw {
        "XXBT" => "BTC",
        "XETH" => "ETH",
        "XLTC" => "LTC",
        "XXLM" => "XLM",
        "XXMR" => "XMR",
        "XXRP" => "XRP",
        "XZEC" => "ZEC",
        "ZUSD" => "USD",
        "ZEUR" => "EUR",
        "ZGBP" => "GBP",
        "ZCAD" => "CAD",
        "ZJPY" => "JPY",
        _ => return raw.strip_prefix('X').or_else(|| raw.strip_prefix('Z')).unwrap_or(raw).to_string(),
    };
    known.to_string()
}

/// One page of accounts (available + on hold) and the cursor of the next one.
fn parse_coinbase_page(json: &Value) -> Option<(Balances, Option<String>)> {
    let accounts = json.get("accounts")?.as_array()?;
    let amount = |a: &Value, field: &str| a.get(field).and_then(|b| b.get("value")).and_then(num).unwrap_or(0.0);
    let balances = accounts
        .iter()
        .filter_map(|a| {
            let currency = a.get("currency")?.as_str()?;
            Some((currency.to_string(), amount(a, "available_balance") + amount(a, "hold")))
        })
        .collect();
    let next = (json.get("has_next").and_then(Value::as_bool) == Some(true))
        .then(|| json.get("cursor").and_then(Value::as_str).filter(|c| !c.is_empty()).map(str::to_string))
        .flatten();
    Some((balances, next))
}

type Balance = Result<Option<f64>, Failure>;

fn parsed(json: &Result<Value, Failure>) -> Result<&Value, Failure> {
    match json {
        Ok(v) => Ok(v),
        Err(Failure::Parse) => Err(Failure::Parse),
        Err(_) => Err(Failure::Network),
    }
}

fn parse_btc(json: &Result<Value, Failure>, address: &str) -> Balance {
    let sats = parsed(json)?.get(address).and_then(|a| a.get("final_balance")).and_then(Value::as_i64);
    sats.map(|s| Some(s as f64 / 1e8)).ok_or(Failure::Parse)
}

fn parse_blockchair(json: &Result<Value, Failure>, address: &str) -> Balance {
    let info = parsed(json)?.get("data").and_then(|d| d.get(address)).and_then(|a| a.get("address"));
    let sats = info.and_then(|i| i.get("balance")).and_then(num);
    sats.map(|s| Some(s / 1e8)).ok_or(Failure::Parse)
}

fn parse_evm(json: &Result<Value, Failure>) -> Balance {
    let wei = parsed(json)?.get("result").and_then(Value::as_str).and_then(hex_to_f64);
    wei.map(|w| Some(w / 1e18)).ok_or(Failure::Parse)
}

/// A 0x hex quantity as a float, without integer overflow.
fn hex_to_f64(hex: &str) -> Option<f64> {
    let digits = hex.strip_prefix("0x").or_else(|| hex.strip_prefix("0X")).unwrap_or(hex);
    digits.chars().try_fold(0.0, |acc, c| Some(acc * 16.0 + f64::from(c.to_digit(16)?)))
}

fn parse_sol(json: &Result<Value, Failure>) -> Balance {
    let lamports = parsed(json)?.pointer("/result/value").and_then(Value::as_i64);
    lamports.map(|l| Some(l as f64 / 1e9)).ok_or(Failure::Parse)
}

fn parse_tron(json: &Result<Value, Failure>) -> Balance {
    let accounts = parsed(json)?.get("data").and_then(Value::as_array).ok_or(Failure::Parse)?;
    // An address that never received anything has no account yet: 0 TRX.
    let sun = accounts.first().and_then(|a| a.get("balance")).and_then(Value::as_i64).unwrap_or(0);
    Ok(Some(sun as f64 / 1e6))
}

fn parse_xrp(json: &Result<Value, Failure>) -> Balance {
    let drops = parsed(json)?.pointer("/result/account_data/Balance").and_then(Value::as_str);
    drops.and_then(|d| d.parse::<f64>().ok()).map(|d| Some(d / 1e6)).ok_or(Failure::Parse)
}

fn parse_cardano(json: &Result<Value, Failure>) -> Balance {
    let first = parsed(json)?.as_array().and_then(|a| a.first()).ok_or(Failure::Parse)?;
    let lovelace = first.get("balance").and_then(Value::as_str).and_then(|b| b.parse::<f64>().ok()).unwrap_or(0.0);
    Ok(Some(lovelace / 1e6))
}

fn parse_cosmos(json: &Result<Value, Failure>) -> Balance {
    let balances = parsed(json)?.get("balances").and_then(Value::as_array).ok_or(Failure::Parse)?;
    Ok(balances
        .iter()
        .filter(|b| b.get("denom").and_then(Value::as_str) == Some("uatom"))
        .filter_map(|b| b.get("amount").and_then(Value::as_str)?.parse::<f64>().ok())
        .find(|amount| *amount > 0.0)
        .map(|uatom| uatom / 1e6))
}

fn parse_near(json: &Result<Value, Failure>) -> Balance {
    let yocto = parsed(json)?.pointer("/result/amount").and_then(Value::as_str).ok_or(Failure::Parse)?;
    // 1 NEAR = 1e24 yoctoNEAR: more digits than an integer holds.
    let value = yocto.chars().fold(0.0, |acc, c| acc * 10.0 + f64::from(c.to_digit(10).unwrap_or(0)));
    Ok(Some(value / 1e24))
}

// ── Prices (Coinbase public API, no key) ──────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
struct PriceInfo {
    price: f64,
    change24h: Option<f64>,
    /// The last seven daily closes, oldest first.
    history: Vec<f64>,
}

/// USD price of everything Coinbase knows, from `rates[X]` = how many X one dollar buys.
fn parse_usd_rates(json: &Value) -> HashMap<String, f64> {
    let Some(rates) = json.pointer("/data/rates").and_then(Value::as_object) else { return HashMap::new() };
    rates
        .iter()
        .filter_map(|(symbol, v)| num(v).filter(|r| *r > 0.0).map(|r| (symbol.to_uppercase(), 1.0 / r)))
        .collect()
}

/// Last price and the change since the 24 h open, in percent.
fn parse_stats(json: &Value) -> Option<(f64, Option<f64>)> {
    let last = num(json.get("last")?)?;
    let open = json.get("open").and_then(num);
    Some((last, open.map(|o| if o > 0.0 { (last - o) / o * 100.0 } else { 0.0 })))
}

/// Daily candles are [time, low, high, open, close, volume], newest first.
fn parse_candles(json: &Value) -> Vec<f64> {
    let Some(candles) = json.as_array() else { return Vec::new() };
    let mut closes: Vec<f64> = candles.iter().take(7).filter_map(|c| c.get(4).and_then(num)).collect();
    closes.reverse();
    closes
}

async fn fetch_market(symbol: String) -> Option<PriceInfo> {
    let http = client(PRICE_TIMEOUT);
    let base = format!("https://api.exchange.coinbase.com/products/{symbol}-USD");
    let stats = send_json(http.get(format!("{base}/stats")).header("Accept", "application/json")).await.ok()?;
    let (price, change24h) = parse_stats(&stats)?;
    let candles = send_json(http.get(format!("{base}/candles?granularity=86400")).header("Accept", "application/json")).await;
    let history = candles.map(|c| parse_candles(&c)).unwrap_or_default();
    Some(PriceInfo { price, change24h, history })
}

async fn fetch_prices(symbols: Vec<String>) -> HashMap<String, PriceInfo> {
    let mut result = HashMap::new();
    if symbols.is_empty() {
        return result;
    }
    // Cash and assets without a Coinbase market still get a price from the rates.
    let rates = send_json(client(PRICE_TIMEOUT).get("https://api.coinbase.com/v2/exchange-rates?currency=USD"))
        .await
        .map(|json| parse_usd_rates(&json))
        .unwrap_or_default();
    for symbol in &symbols {
        let price = if symbol == "USD" { Some(1.0) } else { rates.get(symbol).copied() };
        if let Some(price) = price {
            result.insert(symbol.clone(), PriceInfo { price, change24h: None, history: Vec::new() });
        }
    }
    let markets: Vec<_> = symbols
        .into_iter()
        .filter(|s| !FIAT.contains(&s.as_str()) && is_symbol(s))
        .map(|s| (s.clone(), tauri::async_runtime::spawn(fetch_market(s))))
        .collect();
    for (symbol, task) in markets {
        if let Ok(Some(info)) = task.await {
            result.insert(symbol, info);
        }
    }
    result
}

/// A symbol from an exchange goes into a URL path: letters and digits only.
fn is_symbol(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c.is_ascii_alphanumeric())
}

fn apply_prices(holdings: &mut [Holding], prices: &HashMap<String, PriceInfo>) {
    for h in holdings {
        if let Some(info) = prices.get(&h.symbol) {
            h.price = info.price;
            h.change24h = info.change24h;
            h.price_history = info.history.clone();
        } else if STABLECOINS.contains(&h.symbol.as_str()) {
            h.price = 1.0;
        }
    }
}

// ── The poll ──────────────────────────────────────────────────────────────────

static FETCHING: AtomicBool = AtomicBool::new(false);

enum Source {
    Exchange(Exchange, Creds),
    Wallet(Wallet),
}

/// Every configured account with its holdings priced, as the card's data. None
/// while a fetch is already running.
pub async fn fetch(wallets: Vec<Wallet>) -> Option<Value> {
    if FETCHING.swap(true, Ordering::SeqCst) {
        return None;
    }
    let mut sources: Vec<Source> = Exchange::ALL
        .into_iter()
        .filter_map(|e| Some(Source::Exchange(e, e.stored()?)))
        .collect();
    sources.extend(wallets.into_iter().map(Source::Wallet));

    let tasks: Vec<_> = sources
        .into_iter()
        .map(|source| {
            tauri::async_runtime::spawn(async move {
                match source {
                    Source::Exchange(e, creds) => (e.id().to_string(), e.name().to_string(), fetch_exchange(e, &creds).await),
                    Source::Wallet(w) => (w.address.clone(), w.label.clone(), fetch_wallet(&w).await),
                }
            })
        })
        .collect();
    let mut fetched = Vec::new();
    for task in tasks {
        if let Ok(account) = task.await {
            fetched.push(account);
        }
    }

    let mut symbols: Vec<String> = fetched
        .iter()
        .filter_map(|(_, _, r)| r.as_ref().ok())
        .flatten()
        .map(|h| h.symbol.clone())
        .collect();
    symbols.sort();
    symbols.dedup();
    let prices = fetch_prices(symbols).await;

    let accounts: Vec<Value> = fetched
        .into_iter()
        .map(|(id, label, result)| match result {
            Ok(mut holdings) => {
                apply_prices(&mut holdings, &prices);
                json!({ "id": id, "label": label, "holdings": holdings, "error": null })
            }
            Err(error) => json!({ "id": id, "label": label, "holdings": [], "error": error }),
        })
        .collect();
    FETCHING.store(false, Ordering::SeqCst);
    Some(json!({ "accounts": accounts, "fetchedAt": now_ms() }))
}

// ── Settings → Integrations → Portfolio ───────────────────────────────────────

#[derive(Serialize)]
pub struct Status {
    exchanges: Vec<Exchange>,
    wallets: Vec<Wallet>,
}

fn status(shared: &Shared) -> Status {
    Status {
        exchanges: Exchange::ALL.into_iter().filter(|e| e.stored().is_some()).collect(),
        wallets: shared.settings.lock().unwrap().portfolio_wallets.clone(),
    }
}

fn set_wallets(shared: &Shared, change: impl FnOnce(&mut Vec<Wallet>)) {
    let mut current = shared.settings.lock().unwrap();
    change(&mut current.portfolio_wallets);
    if let Err(err) = settings::save(&current) {
        crate::log::line(format!("could not save settings: {err}"));
    }
}

#[tauri::command]
pub fn portfolio_status(shared: State<Shared>) -> Status {
    status(&shared)
}

#[tauri::command]
pub fn portfolio_address_plausible(kind: WalletKind, address: String) -> bool {
    is_plausible(&address, kind)
}

/// "Test & Save": the keys are kept only once a real fetch went through with them.
#[tauri::command]
pub async fn portfolio_add_exchange(
    app: AppHandle,
    shared: State<'_, Shared>,
    exchange: Exchange,
    key: String,
    secret: String,
    passphrase: String,
) -> Result<Status, String> {
    let creds = Creds { key: key.trim().into(), secret: secret.trim().into(), passphrase: passphrase.trim().into() };
    if !creds.complete(exchange) {
        return Err(t("No credentials found. Add them in Settings."));
    }
    fetch_exchange(exchange, &creds).await?;
    secrets::set_internal(&exchange.entry("key"), &creds.key)?;
    secrets::set_internal(&exchange.entry("secret"), &creds.secret)?;
    if exchange.needs_passphrase() {
        secrets::set_internal(&exchange.entry("passphrase"), &creds.passphrase)?;
    }
    crate::integrations::portfolio_changed(&app);
    Ok(status(&shared))
}

#[tauri::command]
pub fn portfolio_remove_exchange(app: AppHandle, shared: State<Shared>, exchange: Exchange) -> Result<Status, String> {
    for part in ["key", "secret", "passphrase"] {
        secrets::set_internal(&exchange.entry(part), "")?;
    }
    crate::integrations::portfolio_changed(&app);
    Ok(status(&shared))
}

/// "Verify & Save": the address is kept once its balance could be read.
#[tauri::command]
pub async fn portfolio_add_wallet(
    app: AppHandle,
    shared: State<'_, Shared>,
    kind: WalletKind,
    address: String,
    label: String,
) -> Result<Status, String> {
    let address = address.trim().to_string();
    let label = label.trim();
    let label = if label.is_empty() { format!("{}…", address.chars().take(12).collect::<String>()) } else { label.into() };
    let wallet = Wallet { kind, address, label };
    fetch_wallet(&wallet).await?;
    set_wallets(&shared, |list| {
        list.retain(|w| w.address != wallet.address);
        list.push(wallet);
    });
    crate::integrations::portfolio_changed(&app);
    Ok(status(&shared))
}

#[tauri::command]
pub fn portfolio_remove_wallet(app: AppHandle, shared: State<Shared>, address: String) -> Status {
    set_wallets(&shared, |list| list.retain(|w| w.address != address));
    crate::integrations::portfolio_changed(&app);
    status(&shared)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ring::signature::{KeyPair, UnparsedPublicKey, ECDSA_P256_SHA256_FIXED};

    fn pairs(list: Option<Balances>) -> Vec<(String, f64)> {
        let mut list = list.expect("parsed");
        list.sort_by(|a, b| a.0.cmp(&b.0));
        list
    }

    fn p(symbol: &str, amount: f64) -> (String, f64) {
        (symbol.to_string(), amount)
    }

    #[test]
    fn binance_balances_add_free_and_locked() {
        let json = json!({ "balances": [
            { "asset": "BTC", "free": "0.5", "locked": "0.25" },
            { "asset": "ETH", "free": "0.00000000", "locked": "0.00000000" },
            { "asset": "BAD", "free": 3 },
        ]});
        assert_eq!(pairs(parse_binance(&json)), vec![p("BTC", 0.75), p("ETH", 0.0)]);
        assert!(parse_binance(&json!({ "code": -2014 })).is_none());
    }

    #[test]
    fn okx_balances_add_available_and_frozen() {
        let json = json!({ "code": "0", "data": [{ "details": [
            { "ccy": "USDT", "availBal": "120.5", "frozenBal": "10" },
            { "ccy": "SOL", "availBal": "2", "frozenBal": "0" },
        ]}]});
        assert_eq!(pairs(parse_okx(&json)), vec![p("SOL", 2.0), p("USDT", 130.5)]);
        assert!(parse_okx(&json!({ "data": [] })).is_none());
    }

    #[test]
    fn bybit_balances_come_from_the_unified_account() {
        let json = json!({ "retCode": 0, "result": { "list": [{ "coin": [
            { "coin": "BTC", "walletBalance": "0.1" },
            { "coin": "USDC", "walletBalance": "50" },
        ]}]}});
        assert_eq!(pairs(parse_bybit(&json)), vec![p("BTC", 0.1), p("USDC", 50.0)]);
        assert!(parse_bybit(&json!({ "result": { "list": [] } })).is_none());
    }

    #[test]
    fn kraken_names_are_mapped_and_its_errors_reported() {
        let json = json!({ "error": [], "result": { "XXBT": "0.2", "ZUSD": "15.5", "XETH": "1", "ZEUR": "3", "DOT": "4" } });
        assert_eq!(
            pairs(parse_kraken(&json).unwrap()),
            vec![p("BTC", 0.2), p("DOT", 4.0), p("ETH", 1.0), p("EUR", 3.0), p("USD", 15.5)]
        );
        assert_eq!(parse_kraken(&json!({ "error": ["EAPI:Invalid key"] })).unwrap_err(), "EAPI:Invalid key");
        assert!(parse_kraken(&json!({ "error": [] })).unwrap().is_none());
        assert_eq!(kraken_symbol("XXDG"), "XDG");
        assert_eq!(kraken_symbol("ZCHF"), "CHF");
    }

    #[test]
    fn coinbase_pages_add_available_and_hold_and_follow_the_cursor() {
        let page = json!({
            "accounts": [
                { "currency": "BTC", "available_balance": { "value": "0.01", "currency": "BTC" }, "hold": { "value": "0.005" } },
                { "currency": "EUR", "available_balance": { "value": "25" } },
            ],
            "has_next": true, "cursor": "next-page",
        });
        let (balances, next) = parse_coinbase_page(&page).unwrap();
        assert_eq!(pairs(Some(balances)), vec![p("BTC", 0.015), p("EUR", 25.0)]);
        assert_eq!(next.as_deref(), Some("next-page"));
        let last = json!({ "accounts": [], "has_next": false, "cursor": "" });
        assert_eq!(parse_coinbase_page(&last).unwrap().1, None);
        assert!(parse_coinbase_page(&json!({ "error": "unauthorized" })).is_none());
    }

    // A throwaway key made for this test only (openssl ecparam -name prime256v1 -genkey).
    const TEST_SEC1: &str = "-----BEGIN EC PRIVATE KEY-----\nMHcCAQEEICWQzRRl27egM6lRDH4U1p/C+wBrTqmW9vBd01R4uOHpoAoGCCqGSM49\nAwEHoUQDQgAEC0fx5z/5JZayxiATIWrksNwVJnuV77cU7H2fTFYPdZPsm/Ai5tW5\nzy0Zb5kI0cAgKJMEywzk3XlThzZsJIDP6w==\n-----END EC PRIVATE KEY-----\n";
    const TEST_PKCS8: &str = "-----BEGIN PRIVATE KEY-----\nMIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgJZDNFGXbt6AzqVEM\nfhTWn8L7AGtOqZb28F3TVHi44emhRANCAAQLR/HnP/kllrLGIBMhauSw3BUme5Xv\ntxTsfZ9MVg91k+yb8CLm1bnPLRlvmQjRwCAokwTLDOTdeVOHNmwkgM/r\n-----END PRIVATE KEY-----\n";

    #[test]
    fn a_coinbase_key_is_read_however_it_was_pasted() {
        let reference = coinbase_signer(TEST_PKCS8).expect("PKCS#8");
        let public = reference.public_key().as_ref().to_vec();
        let escaped = format!("\"{}\"", TEST_SEC1.replace('\n', "\\n"));
        let one_line = TEST_SEC1.replace('\n', " ");
        for pasted in [TEST_SEC1, escaped.as_str(), one_line.as_str()] {
            let signer = coinbase_signer(pasted).expect(pasted);
            assert_eq!(signer.public_key().as_ref(), public.as_slice());
        }
        assert!(coinbase_signer("not a key").is_none());
        assert!(coinbase_signer("-----BEGIN EC PRIVATE KEY-----\nAAAA\n-----END EC PRIVATE KEY-----").is_none());
    }

    #[test]
    fn the_coinbase_jwt_is_signed_with_the_documented_claims() {
        let signer = coinbase_signer(TEST_SEC1).unwrap();
        let jwt = coinbase_jwt("organizations/x/apiKeys/y", &signer, "GET", COINBASE_HOST, COINBASE_PATH, 1_700_000_000).unwrap();
        let parts: Vec<&str> = jwt.split('.').collect();
        assert_eq!(parts.len(), 3);
        assert!(!jwt.contains('=') && !jwt.contains('+') && !jwt.contains('/'));
        let decode = |s: &str| {
            let std = s.replace('-', "+").replace('_', "/");
            let padded = format!("{std}{}", "=".repeat((4 - std.len() % 4) % 4));
            serde_json::from_slice::<Value>(&crate::recap::decode_base64(&padded).unwrap()).unwrap()
        };
        let header = decode(parts[0]);
        assert_eq!(header["alg"], "ES256");
        assert_eq!(header["kid"], "organizations/x/apiKeys/y");
        assert_eq!(header["nonce"].as_str().unwrap().len(), 32);
        let claims = decode(parts[1]);
        assert_eq!(claims["iss"], "cdp");
        assert_eq!(claims["sub"], "organizations/x/apiKeys/y");
        assert_eq!(claims["exp"], 1_700_000_120);
        assert_eq!(claims["uri"], "GET api.coinbase.com/api/v3/brokerage/accounts");
        let sig_std = parts[2].replace('-', "+").replace('_', "/");
        let sig = crate::recap::decode_base64(&format!("{sig_std}{}", "=".repeat((4 - sig_std.len() % 4) % 4))).unwrap();
        assert_eq!(sig.len(), 64);
        let public = UnparsedPublicKey::new(&ECDSA_P256_SHA256_FIXED, signer.public_key().as_ref().to_vec());
        assert!(public.verify(format!("{}.{}", parts[0], parts[1]).as_bytes(), &sig).is_ok());
    }

    #[test]
    fn signatures_match_known_hmac_vectors() {
        // RFC 4231 test case 2.
        let mac = hmac_sign(hmac::HMAC_SHA256, b"Jefe", b"what do ya want for nothing?");
        assert_eq!(hex(&mac), "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843");
        let mac = hmac_sign(hmac::HMAC_SHA512, b"Jefe", b"what do ya want for nothing?");
        assert!(hex(&mac).starts_with("164b7a7bfcf819e2e395fbe73b56e0a387bd64222e831fd610270cd7ea250554"));
    }

    #[test]
    fn okx_timestamps_are_utc_with_milliseconds() {
        assert_eq!(iso8601_utc(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(iso8601_utc(1_607_418_537_715), "2020-12-08T09:08:57.715Z");
        assert_eq!(iso8601_utc(951_782_400_001), "2000-02-29T00:00:00.001Z");
    }

    #[test]
    fn wallet_balances_are_read_in_whole_coins() {
        let ok = |v: Value| Ok::<Value, Failure>(v);
        let addr = "bc1qexampleexampleexample";
        assert_eq!(parse_btc(&ok(json!({ addr: { "final_balance": 150_000_000 } })), addr).ok(), Some(Some(1.5)));
        assert_eq!(
            parse_blockchair(&ok(json!({ "data": { "Dexample": { "address": { "balance": "250000000" } } } })), "Dexample").ok(),
            Some(Some(2.5))
        );
        assert_eq!(parse_evm(&ok(json!({ "result": "0xde0b6b3a7640000" }))).ok(), Some(Some(1.0)));
        assert_eq!(parse_sol(&ok(json!({ "result": { "value": 2_000_000_000u64 } }))).ok(), Some(Some(2.0)));
        assert_eq!(parse_tron(&ok(json!({ "data": [{ "balance": 3_000_000 }] }))).ok(), Some(Some(3.0)));
        assert_eq!(parse_tron(&ok(json!({ "data": [] }))).ok(), Some(Some(0.0)));
        assert_eq!(parse_xrp(&ok(json!({ "result": { "account_data": { "Balance": "4500000" } } }))).ok(), Some(Some(4.5)));
        assert_eq!(parse_cardano(&ok(json!([{ "balance": "7000000" }]))).ok(), Some(Some(7.0)));
        assert_eq!(
            parse_cosmos(&ok(json!({ "balances": [{ "denom": "ibc/X", "amount": "9" }, { "denom": "uatom", "amount": "1500000" }] }))).ok(),
            Some(Some(1.5))
        );
        assert_eq!(parse_cosmos(&ok(json!({ "balances": [] }))).ok(), Some(None));
        let near = parse_near(&ok(json!({ "result": { "amount": "2500000000000000000000000" } }))).ok().flatten().unwrap();
        assert!((near - 2.5).abs() < 1e-9);
    }

    #[test]
    fn a_wallet_answer_of_the_wrong_shape_is_a_parse_error_and_a_failed_request_a_network_one() {
        let ok = |v: Value| Ok::<Value, Failure>(v);
        assert!(matches!(parse_btc(&ok(json!({})), "x"), Err(Failure::Parse)));
        assert!(matches!(parse_evm(&ok(json!({ "error": { "message": "rate limited" } }))), Err(Failure::Parse)));
        assert!(matches!(parse_evm(&ok(json!({ "result": "0xzz" }))), Err(Failure::Parse)));
        assert!(matches!(parse_cardano(&ok(json!([]))), Err(Failure::Parse)));
        assert!(matches!(parse_sol(&Err(Failure::Status(429, String::new()))), Err(Failure::Network)));
        assert!(matches!(parse_near(&Err(Failure::Network)), Err(Failure::Network)));
    }

    #[test]
    fn prices_rates_stats_and_candles() {
        let rates = parse_usd_rates(&json!({ "data": { "currency": "USD", "rates": { "EUR": "0.8", "btc": "0.00002", "ZERO": "0" } } }));
        assert!((rates["EUR"] - 1.25).abs() < 1e-9);
        assert!((rates["BTC"] - 50_000.0).abs() < 1e-6);
        assert!(!rates.contains_key("ZERO"));
        assert_eq!(parse_stats(&json!({ "last": "110", "open": "100" })), Some((110.0, Some(10.0))));
        assert_eq!(parse_stats(&json!({ "last": 5.0 })), Some((5.0, None)));
        assert_eq!(parse_stats(&json!({ "message": "NotFound" })), None);
        let candles: Vec<Value> = (0..9).map(|i| json!([i, 0, 0, 0, format!("{}", 100 - i), 0])).collect();
        assert_eq!(parse_candles(&Value::Array(candles)), vec![94.0, 95.0, 96.0, 97.0, 98.0, 99.0, 100.0]);
        assert!(parse_candles(&json!({ "message": "x" })).is_empty());
    }

    #[test]
    fn stablecoins_without_a_price_count_as_one_dollar() {
        let mut list = holdings(vec![p("USDT", 10.0), p("BTC", 0.5), p("XYZ", 3.0), p("DUST", 0.0000001)]);
        let prices = HashMap::from([(
            "BTC".to_string(),
            PriceInfo { price: 40_000.0, change24h: Some(-2.0), history: vec![1.0, 2.0] },
        )]);
        apply_prices(&mut list, &prices);
        let get = |s: &str| list.iter().find(|h| h.symbol == s).unwrap();
        assert_eq!(list.len(), 3);
        assert_eq!(get("USDT").price, 1.0);
        assert_eq!(get("BTC").price, 40_000.0);
        assert_eq!(get("BTC").change24h, Some(-2.0));
        assert_eq!(get("XYZ").price, 0.0);
    }

    #[test]
    fn addresses_are_checked_locally_before_the_network() {
        use WalletKind::*;
        let good = [
            (Bitcoin, "bc1qexampleaddress0000000000"),
            (Bitcoin, "1ExampleAddress0000000000000"),
            (Dogecoin, "DExampleAddress0000000000000"),
            (Litecoin, "ltc1qexampleaddress000000000"),
            (Ethereum, "0x00000000000000000000000000000000000000ab"),
            (Base, "0xABCDEF0000000000000000000000000000000000"),
            (Solana, "ExampleSolanaAddress11111111111111"),
            (Tron, "TExampleTronAddress000000000000000"),
            (Xrp, "rExampleXrpAddress00000000"),
            (Cardano, "addr1qexampleexampleexampleexampleexampleexampleexample"),
            (Cosmos, "cosmos1exampleexampleexampleexampleexample000"),
            (Near, "alice.near"),
            (Near, "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"),
        ];
        for (kind, address) in good {
            assert!(is_plausible(address, kind), "{kind:?} {address}");
        }
        let bad = [
            (Bitcoin, "2Example"),
            (Ethereum, "0x123"),
            (Ethereum, "0xGG000000000000000000000000000000000000ab"),
            (Tron, "TShort"),
            (Cosmos, "cosmos1short"),
            (Near, "alice"),
            (Solana, "../../etc/passwd/../../../../../../x"),
            (Near, "alice.near?x=1"),
        ];
        for (kind, address) in bad {
            assert!(!is_plausible(address, kind), "{kind:?} {address}");
        }
    }

    #[test]
    fn wallets_and_kinds_keep_the_macs_raw_values() {
        let wallet = Wallet { kind: WalletKind::BnbChain, address: "0xabc".into(), label: "Savings".into() };
        assert_eq!(serde_json::to_value(&wallet).unwrap(), json!({ "kind": "bnbChain", "address": "0xabc", "label": "Savings" }));
        assert_eq!(serde_json::to_value(Exchange::Okx).unwrap(), json!("okx"));
        assert_eq!(Exchange::Coinbase.entry("secret"), "portfolio-coinbase-secret");
        for e in Exchange::ALL {
            for part in ["key", "secret", "passphrase"] {
                assert!(secrets::is_internal(&e.entry(part)), "{}", e.entry(part));
            }
        }
    }
}
