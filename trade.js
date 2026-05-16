// ============================================================
//  BingX Auto Trade Bot — BTC-USDT
//  Run: node trade.js        → DEMO mode (safe)
//  Run: node trade.js live   → LIVE mode (real money)
// ============================================================

require("dotenv").config();
const WebSocket = require("ws");
const https = require("https");
const crypto = require("crypto");

// ─────────────────────────────────────────
//  CONFIG
// ─────────────────────────────────────────
const CONFIG = {
  TELEGRAM_BOT_TOKEN: process.env.TELEGRAM_BOT_TOKEN,
  TELEGRAM_CHAT_ID: process.env.TELEGRAM_CHAT_ID,
  BINGX_API_KEY: process.env.BINGX_API_KEY,
  BINGX_API_SECRET: process.env.BINGX_API_SECRET,

  SYMBOLS: ["BTC-USDT"],
  TIMEFRAME: "5m",
  RISK_USDT: 2,
  RR: 3,
  LAST_CLOSE_COOLDOWN_MS: 15 * 60 * 1000, // 15 min after last CLOSED trade

  // Detection — identical to alert script
  LOOKBACK: 4,
  BODY_MULTIPLIER: 2.0,
  MIN_BODY_PCT: 0.1,
  RANGE_THRESHOLD: 0.35,
  CLUSTER_TOUCH_MIN: 2,
  CLUSTER_ZONE_PCT: 0.12,
  VOLUME_MULTIPLIER: 1.4,

  MIN_QTY: 0.0001,
  QTY_PRECISION: 4,
  PRICE_PRECISION: 1
};

// ─────────────────────────────────────────
//  MODE
// ─────────────────────────────────────────
const MODE = process.argv[2] === "live" ? "live" : "demo";
const BINGX_BASE = MODE === "live" ? "open-api.bingx.com" : "open-api-vst.bingx.com";

console.log(`\n🤖 Trade Bot starting in ${MODE.toUpperCase()} mode`);
console.log(`📡 BingX: ${BINGX_BASE}\n`);

// ─────────────────────────────────────────
//  STATE
// ─────────────────────────────────────────
const candleStore = {}; // binance symbol → candles[]
const lastCloseTime = {}; // bingx symbol   → timestamp of last CLOSED trade
const openOrderIds = {}; // bingx symbol   → orderId if trade currently open

CONFIG.SYMBOLS.forEach((s) => {
  const bSym = s.replace("-", "").toLowerCase();
  candleStore[bSym] = [];
  lastCloseTime[s] = 0;
  openOrderIds[s] = null;
});

// ─────────────────────────────────────────
//  HELPERS
// ─────────────────────────────────────────
const bodySize = (c) => Math.abs(c.close - c.open);
const bodyPct = (c) => (bodySize(c) / c.open) * 100;
const isBullish = (c) => c.close > c.open;
const isBearish = (c) => c.close < c.open;
const avg = (arr) => arr.reduce((a, b) => a + b, 0) / arr.length;
const toIST = (ts) => new Date(ts).toLocaleString("en-IN", { timeZone: "Asia/Kolkata", hour12: true });
const round = (v, d) => parseFloat(v.toFixed(d));

// ─────────────────────────────────────────
//  BINGX SIGNATURE
// ─────────────────────────────────────────
function buildQueryAndSign(params) {
  params.timestamp = Date.now();
  const qs = Object.keys(params)
    .sort()
    .map((k) => `${k}=${params[k]}`)
    .join("&");
  const sig = crypto.createHmac("sha256", CONFIG.BINGX_API_SECRET).update(qs).digest("hex");
  return `${qs}&signature=${sig}`;
}

// ─────────────────────────────────────────
//  BINGX REST
// ─────────────────────────────────────────
function bingxRequest(method, path, params) {
  return new Promise((resolve, reject) => {
    const qs = buildQueryAndSign(params);
    const opts = {
      hostname: BINGX_BASE,
      path: `${path}?${qs}`,
      method,
      headers: { "X-BX-APIKEY": CONFIG.BINGX_API_KEY }
    };
    const req = https.request(opts, (res) => {
      let d = "";
      res.on("data", (c) => (d += c));
      res.on("end", () => {
        try {
          resolve(JSON.parse(d));
        } catch (e) {
          reject(new Error(`JSON parse: ${d}`));
        }
      });
    });
    req.on("error", reject);
    req.end();
  });
}

// ─────────────────────────────────────────
//  CHECK IF TRADE IS STILL OPEN on BingX
//  Returns true if position exists for symbol
// ─────────────────────────────────────────
async function isTradeOpen(bingxSym) {
  try {
    const res = await bingxRequest("GET", "/openApi/swap/v2/trade/openOrders", {
      symbol: bingxSym
    });
    if (res.code !== 0) return false;
    const orders = res.data?.orders || [];
    // Also check open positions
    const posRes = await bingxRequest("GET", "/openApi/swap/v2/user/positions", {
      symbol: bingxSym
    });
    const positions = posRes.data || [];
    const hasPos = positions.some((p) => p.symbol === bingxSym && parseFloat(p.positionAmt) !== 0);
    return hasPos || orders.length > 0;
  } catch (e) {
    console.error("isTradeOpen error:", e.message);
    return false; // if check fails, allow trade
  }
}

// ─────────────────────────────────────────
//  TELEGRAM
// ─────────────────────────────────────────
function sendTelegram(msg) {
  const body = JSON.stringify({
    chat_id: CONFIG.TELEGRAM_CHAT_ID,
    text: msg,
    parse_mode: "HTML"
  });
  const opts = {
    hostname: "api.telegram.org",
    path: `/bot${CONFIG.TELEGRAM_BOT_TOKEN}/sendMessage`,
    method: "POST",
    headers: { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(body) }
  };
  const req = https.request(opts, (res) => {
    let d = "";
    res.on("data", (c) => (d += c));
    res.on("end", () => {
      const p = JSON.parse(d);
      if (!p.ok) console.error("TG:", p.description);
    });
  });
  req.on("error", (e) => console.error("TG error:", e.message));
  req.write(body);
  req.end();
}

// ─────────────────────────────────────────
//  PATTERN DETECTION — identical to index.js
// ─────────────────────────────────────────
function detect(candles) {
  if (candles.length < CONFIG.LOOKBACK + 1) return null;

  const prev = candles.slice(-(CONFIG.LOOKBACK + 1), -1);
  const current = candles[candles.length - 1];

  const avgBody = avg(prev.map(bodySize));
  const avgVol = avg(prev.map((c) => c.volume));
  const curBody = bodySize(current);
  const curVol = current.volume;

  if (bodyPct(current) < CONFIG.MIN_BODY_PCT) return null;
  if (curBody < avgBody * CONFIG.BODY_MULTIPLIER) return null;
  if (curVol < avgVol * CONFIG.VOLUME_MULTIPLIER) return null;

  const highs = prev.map((c) => c.high);
  const lows = prev.map((c) => c.low);
  const hiMax = Math.max(...highs);
  const loMin = Math.min(...lows);
  const rangePct = ((hiMax - loMin) / loMin) * 100;
  const isSideways = rangePct < CONFIG.RANGE_THRESHOLD;

  const topLevel = Math.max(...prev.map((c) => Math.max(c.open, c.close)));
  const botLevel = Math.min(...prev.map((c) => Math.min(c.open, c.close)));

  const touchingLevel = (c, level) => {
    const pct = CONFIG.CLUSTER_ZONE_PCT / 100;
    return Math.abs(c.open - level) / level < pct || Math.abs(c.close - level) / level < pct;
  };

  const touchTop = prev.filter((c) => touchingLevel(c, topLevel)).length;
  const touchBot = prev.filter((c) => touchingLevel(c, botLevel)).length;
  const isLevelHeld = touchTop >= CONFIG.CLUSTER_TOUCH_MIN || touchBot >= CONFIG.CLUSTER_TOUCH_MIN;

  if (!isSideways && !isLevelHeld) return null;

  const bullish = isBullish(current) && current.close > topLevel;
  const bearish = isBearish(current) && current.close < botLevel;
  if (!bullish && !bearish) return null;

  return {
    direction: bullish ? "BULLISH" : "BEARISH",
    pattern: isSideways ? "Sideways Breakout" : "Level Break Momentum",
    candle: current,
    entry: current.close,
    sl: bullish ? current.low : current.high,
    bodyPct: bodyPct(current).toFixed(3),
    bodyVsAvg: (curBody / avgBody).toFixed(1),
    volVsAvg: (curVol / avgVol).toFixed(1),
    rangePct: rangePct.toFixed(3)
  };
}

// ─────────────────────────────────────────
//  POSITION SIZE
// ─────────────────────────────────────────
function calcQty(entry, sl) {
  const dist = Math.abs(entry - sl);
  if (!dist) return null;
  const qty = round(CONFIG.RISK_USDT / dist, CONFIG.QTY_PRECISION);
  return qty < CONFIG.MIN_QTY ? CONFIG.MIN_QTY : qty;
}

// ─────────────────────────────────────────
//  PLACE ORDER
// ─────────────────────────────────────────
async function placeOrder(signal, bingxSym) {
  const { direction, entry, sl } = signal;
  const qty = calcQty(entry, sl);
  if (!qty) {
    console.error("SL distance 0, skip");
    return null;
  }

  const slDist = Math.abs(entry - sl);
  const tp = direction === "BULLISH" ? round(entry + CONFIG.RR * slDist, CONFIG.PRICE_PRECISION) : round(entry - CONFIG.RR * slDist, CONFIG.PRICE_PRECISION);
  const slPrice = round(sl, CONFIG.PRICE_PRECISION);

  const params = {
    symbol: bingxSym,
    side: direction === "BULLISH" ? "BUY" : "SELL",
    positionSide: direction === "BULLISH" ? "LONG" : "SHORT",
    type: "MARKET",
    quantity: qty,
    stopLoss: JSON.stringify({ type: "STOP_MARKET", stopPrice: slPrice, price: slPrice, workingType: "MARK_PRICE" }),
    takeProfit: JSON.stringify({ type: "TAKE_PROFIT_MARKET", stopPrice: tp, price: tp, workingType: "MARK_PRICE" })
  };

  console.log(`\n📤 Placing ${MODE.toUpperCase()} order | ${direction} ${qty} ${bingxSym} SL:${slPrice} TP:${tp}`);

  try {
    const res = await bingxRequest("POST", "/openApi/swap/v2/trade/order", params);
    if (res.code === 0) {
      const orderId = res.data?.order?.orderId || "N/A";
      console.log(`✅ Order placed! orderId: ${orderId}`);
      return { qty, slPrice, tp, orderId, slDist };
    } else {
      console.error(`❌ BingX ${res.code}: ${res.msg}`);
      sendTelegram(`❌ <b>Order failed</b> — ${bingxSym}\n${res.code}: ${res.msg}`);
      return null;
    }
  } catch (e) {
    console.error("Order error:", e.message);
    sendTelegram(`❌ <b>Order error</b> — ${bingxSym}\n${e.message}`);
    return null;
  }
}

// ─────────────────────────────────────────
//  HANDLE SIGNAL
// ─────────────────────────────────────────
async function handleSignal(binanceSym, signal) {
  const bingxSym = binanceSym.replace("usdt", "-USDT").toUpperCase();
  const now = Date.now();

  // ── Guard 1: Is a trade already open on BingX?
  const tradeOpen = await isTradeOpen(bingxSym);
  if (tradeOpen) {
    console.log(`⏭ [${bingxSym}] Trade already open — skipping signal`);
    sendTelegram(`⏭ <b>Signal detected but skipped — ${bingxSym}</b>\n` + `Reason: Trade already open\n` + `${signal.direction === "BULLISH" ? "🟢" : "🔴"} ${signal.direction} | ${signal.pattern}\n` + `💰 Entry: ${signal.entry} | 🕒 ${toIST(signal.candle.openTime)}`);
    return;
  }

  // ── Guard 2: Last closed trade was less than 15 min ago?
  if (now - lastCloseTime[bingxSym] < CONFIG.LAST_CLOSE_COOLDOWN_MS) {
    const minsAgo = Math.round((now - lastCloseTime[bingxSym]) / 60000);
    console.log(`⏭ [${bingxSym}] Last trade closed ${minsAgo}m ago — cooldown active`);
    sendTelegram(`⏭ <b>Signal detected but skipped — ${bingxSym}</b>\n` + `Reason: Last trade closed ${minsAgo}m ago (cooldown 15m)\n` + `${signal.direction === "BULLISH" ? "🟢" : "🔴"} ${signal.direction} | ${signal.pattern}\n` + `💰 Entry: ${signal.entry} | 🕒 ${toIST(signal.candle.openTime)}`);
    return;
  }

  // ── Place order
  const order = await placeOrder(signal, bingxSym);
  const modeTag = MODE === "demo" ? "🧪 DEMO" : "💰 LIVE";
  const dir = signal.direction === "BULLISH" ? "🟢 BULLISH" : "🔴 BEARISH";

  const msg = order
    ? `<b>${modeTag} — TRADE PLACED 🚀 ${bingxSym}</b>
${dir} | ${signal.pattern}

💰 <b>Entry:</b> ${signal.entry}
🛑 <b>Stop Loss:</b> ${order.slPrice}
🎯 <b>Target (${CONFIG.RR}R):</b> ${order.tp}
📦 <b>Qty:</b> ${order.qty} BTC
💵 <b>Risk:</b> $${CONFIG.RISK_USDT}
📊 <b>Body:</b> ${signal.bodyPct}% | ${signal.bodyVsAvg}x avg
📦 <b>Volume:</b> ${signal.volVsAvg}x avg
🕒 <b>Candle:</b> ${toIST(signal.candle.openTime)}
🔖 <b>OrderId:</b> ${order.orderId}`
    : `<b>⚠️ Signal detected — order FAILED — ${bingxSym}</b>\n${dir} | Entry: ${signal.entry}`;

  console.log("\n" + "=".repeat(55));
  console.log(msg.replace(/<[^>]+>/g, ""));
  console.log("=".repeat(55));
  sendTelegram(msg);

  // Mark trade as open — will be cleared when position closes
  if (order) {
    openOrderIds[bingxSym] = order.orderId;
    // Poll every 30s to detect when position closes, then update lastCloseTime
    pollForClose(bingxSym);
  }
}

// ─────────────────────────────────────────
//  POLL FOR POSITION CLOSE
//  Checks every 30s if position is still open
//  When closed → sets lastCloseTime
// ─────────────────────────────────────────
function pollForClose(bingxSym) {
  const interval = setInterval(async () => {
    try {
      const stillOpen = await isTradeOpen(bingxSym);
      if (!stillOpen) {
        console.log(`✅ [${bingxSym}] Position closed — starting 15m cooldown`);
        lastCloseTime[bingxSym] = Date.now();
        openOrderIds[bingxSym] = null;
        clearInterval(interval);
        sendTelegram(`✅ <b>Position closed — ${bingxSym}</b>\n15 min cooldown started`);
      }
    } catch (e) {
      console.error("pollForClose error:", e.message);
    }
  }, 30 * 1000); // check every 30 seconds
}

// ─────────────────────────────────────────
//  SEED CANDLES
// ─────────────────────────────────────────
function fetchSeedCandles(symbol) {
  return new Promise((resolve, reject) => {
    const path = `/api/v3/klines?symbol=${symbol.toUpperCase()}&interval=${CONFIG.TIMEFRAME}&limit=${CONFIG.LOOKBACK + 1}`;

    console.log(`📥 Fetching seed candles for ${symbol.toUpperCase()}`);
    console.log(`🌐 URL: https://api.binance.com${path}`);

    const req = https.request(
      {
        hostname: "api.binance.com",
        path,
        method: "GET"
      },
      (res) => {
        console.log(`📡 BINANCE STATUS: ${res.statusCode}`);
        console.log("📡 BINANCE HEADERS:", res.headers);

        let d = "";

        res.on("data", (c) => {
          d += c;
        });

        res.on("end", () => {
          console.log("\n================ BINANCE RESPONSE ================");
          console.log(d);
          console.log("==================================================\n");

          try {
            const rows = JSON.parse(d);

            console.log("🔍 TYPE:", typeof rows);
            console.log("🔍 IS ARRAY:", Array.isArray(rows));

            // If Binance did NOT return candle array
            if (!Array.isArray(rows)) {
              console.log("\n❌ Binance returned NON-ARRAY response");
              console.log("❌ Full Parsed Response:");
              console.log(rows);
              console.log("=====================================\n");

              return reject(new Error("Binance returned non-array response"));
            }

            const closed = rows.slice(0, -1).map((r) => ({
              openTime: r[0],
              open: parseFloat(r[1]),
              high: parseFloat(r[2]),
              low: parseFloat(r[3]),
              close: parseFloat(r[4]),
              volume: parseFloat(r[5])
            }));

            candleStore[symbol].push(...closed);

            console.log(`✅ [${symbol.toUpperCase()}] Seeded ${closed.length} candles`);

            resolve();
          } catch (e) {
            console.log("\n=========== PARSE ERROR ===========");
            console.log("❌ ERROR:", e.message);
            console.log("❌ RAW BODY:");
            console.log(d);
            console.log("===================================\n");

            reject(e);
          }
        });
      }
    );

    req.on("error", (err) => {
      console.log("\n=========== REQUEST ERROR ===========");
      console.log(err);
      console.log("=====================================\n");

      reject(err);
    });

    req.end();
  });
}

// ─────────────────────────────────────────
//  WEBSOCKET
// ─────────────────────────────────────────
async function startWebSocket() {
  const binanceSyms = CONFIG.SYMBOLS.map((s) => s.replace("-", "").toLowerCase());
  await Promise.all(binanceSyms.map(fetchSeedCandles));
  console.log("✅ Seed complete. Connecting...\n");

  const ws = new WebSocket(`wss://stream.binance.com:9443/stream?streams=${binanceSyms.map((s) => `${s}@kline_${CONFIG.TIMEFRAME}`).join("/")}`);

  ws.on("open", () => {
    console.log("✅ Connected.\n");
    sendTelegram(`🤖 <b>Trade Bot — ${MODE.toUpperCase()}</b>\n` + `Symbols: ${CONFIG.SYMBOLS.join(", ")} | TF: ${CONFIG.TIMEFRAME}\n` + `Risk: $${CONFIG.RISK_USDT} | Target: ${CONFIG.RR}R`);
  });

  ws.on("message", async (raw) => {
    try {
      const msg = JSON.parse(raw);
      const kline = msg.data?.k;
      if (!kline || !kline.x) return;

      const sym = kline.s.toLowerCase();
      const candle = {
        openTime: kline.t,
        open: parseFloat(kline.o),
        high: parseFloat(kline.h),
        low: parseFloat(kline.l),
        close: parseFloat(kline.c),
        volume: parseFloat(kline.v)
      };

      candleStore[sym].push(candle);
      if (candleStore[sym].length > 50) candleStore[sym].shift();

      console.log(`[${sym.toUpperCase()}] ${toIST(candle.openTime)} | C: ${candle.close}`);

      const signal = detect(candleStore[sym]);
      if (signal) await handleSignal(sym, signal);
    } catch (e) {
      console.error("Msg error:", e.message);
    }
  });

  ws.on("close", () => {
    console.warn("WS closed. Reconnecting...");
    setTimeout(startWebSocket, 5000);
  });
  ws.on("error", (err) => {
    console.error("WS error:", err.message);
    ws.terminate();
  });
}

// ─────────────────────────────────────────
//  VALIDATE & START
// ─────────────────────────────────────────
const required = ["TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID", "BINGX_API_KEY", "BINGX_API_SECRET"];
const missing = required.filter((k) => !CONFIG[k]);
if (missing.length) {
  console.error(`❌ Missing in .env: ${missing.join(", ")}`);
  process.exit(1);
}

startWebSocket();
