// ============================================================
//  BingX Auto Trade Bot — LIVE ONLY
//  Run: node trade.js
// ============================================================

require("dotenv").config();

const WebSocket = require("ws");
const https = require("https");
const crypto = require("crypto");

// ─────────────────────────────────────────
// CONFIG
// ─────────────────────────────────────────
const CONFIG = {
  TELEGRAM_BOT_TOKEN: process.env.TELEGRAM_BOT_TOKEN,
  TELEGRAM_CHAT_ID: process.env.TELEGRAM_CHAT_ID,

  BINGX_API_KEY: process.env.BINGX_API_KEY,
  BINGX_API_SECRET: process.env.BINGX_API_SECRET,

  SYMBOLS: ["BTC-USDT"],

  TIMEFRAME: process.env.TIMEFRAME || "5m",

  // REAL TRADE RISK
  RISK_USDT: process.env.RISK_USDT || 1,
  RR: 3,

  LAST_CLOSE_COOLDOWN_MS: 15 * 60 * 1000,

  // DETECTION SETTINGS
  LOOKBACK: 4,
  BODY_MULTIPLIER: 2.5,
  MIN_BODY_PCT: 0.1,
  RANGE_THRESHOLD: 0.35,
  CLUSTER_TOUCH_MIN: 2,
  CLUSTER_ZONE_PCT: 0.12,
  VOLUME_MULTIPLIER: 1.4,

  MIN_QTY: 0.0001,
  QTY_PRECISION: 4,
  PRICE_PRECISION: 1,

  // ───── TEST ORDER ─────
  TEST_ORDER: process.env.TEST_ORDER === "true" ? true : false,

  TEST_LEVERAGE: 150,
  TEST_QTY: 0.0001,
  TEST_SL_DIST: 50,
  TEST_TP_DIST: 50
};

// ─────────────────────────────────────────
// LIVE ONLY
// ─────────────────────────────────────────
const MODE = "LIVE";
const BINGX_BASE = "open-api.bingx.com";

console.log(`\n🤖 Trade Bot starting in LIVE mode`);
console.log(`📡 BingX host: ${BINGX_BASE}\n`);

// ─────────────────────────────────────────
// STATE
// ─────────────────────────────────────────
const candleStore = {};
const lastCloseTime = {};
const openOrderIds = {};

CONFIG.SYMBOLS.forEach((s) => {
  const bSym = s.replace("-", "").toLowerCase();

  candleStore[bSym] = [];
  lastCloseTime[s] = 0;
  openOrderIds[s] = null;
});

// ─────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────
const bodySize = (c) => Math.abs(c.close - c.open);

const bodyPct = (c) => (bodySize(c) / c.open) * 100;

const isBullish = (c) => c.close > c.open;

const isBearish = (c) => c.close < c.open;

const avg = (arr) => arr.reduce((a, b) => a + b, 0) / arr.length;

const round = (v, d) => parseFloat(v.toFixed(d));

const toIST = (ts) =>
  new Date(ts).toLocaleString("en-IN", {
    timeZone: "Asia/Kolkata",
    hour12: true
  });

// ─────────────────────────────────────────
// SERVER TIME SYNC
// ─────────────────────────────────────────
let serverTimeOffset = 0;

async function syncServerTime() {
  return new Promise((resolve) => {
    const req = https.request(
      {
        hostname: BINGX_BASE,
        path: "/openApi/swap/v2/server/time",
        method: "GET"
      },
      (res) => {
        let d = "";

        res.on("data", (c) => (d += c));

        res.on("end", () => {
          try {
            const json = JSON.parse(d);

            const serverTime = json.data?.serverTime || json.serverTime;

            if (serverTime) {
              serverTimeOffset = serverTime - Date.now();

              console.log(`🕒 Server time synced. Offset: ${serverTimeOffset}ms`);
            }
          } catch (e) {
            console.warn("⚠️ Time sync failed");
          }

          resolve();
        });
      }
    );

    req.on("error", () => resolve());

    req.end();
  });
}

// ─────────────────────────────────────────
// SIGNATURE
// ─────────────────────────────────────────
function buildQueryAndSign(params) {
  params.timestamp = Date.now() + serverTimeOffset;

  // RAW query for signature
  const rawQuery = Object.keys(params)
    .sort()
    .map((k) => `${k}=${params[k]}`)
    .join("&");

  // SIGN RAW
  const signature = crypto.createHmac("sha256", CONFIG.BINGX_API_SECRET).update(rawQuery).digest("hex");

  // ENCODE ONLY FOR URL
  const encodedQuery = Object.keys(params)
    .sort()
    .map((k) => {
      return `${k}=${encodeURIComponent(params[k])}`;
    })
    .join("&");

  return `${encodedQuery}&signature=${signature}`;
}

// ─────────────────────────────────────────
// BINGX REQUEST
// ─────────────────────────────────────────
function bingxRequest(method, path, params = {}) {
  return new Promise((resolve, reject) => {
    const qs = buildQueryAndSign(params);

    const options = {
      hostname: BINGX_BASE,
      path: `${path}?${qs}`,
      method,
      headers: {
        "X-BX-APIKEY": CONFIG.BINGX_API_KEY
      }
    };

    const req = https.request(options, (res) => {
      let d = "";

      res.on("data", (c) => (d += c));

      res.on("end", () => {
        console.log(`📡 STATUS: ${res.statusCode} | PATH: ${path}`);

        if (!d || !d.trim()) {
          console.error(`📦 RAW: [EMPTY]`);

          return reject(new Error(`Empty response (HTTP ${res.statusCode})`));
        }

        console.log(`📦 RAW: ${d}`);

        try {
          const parsed = JSON.parse(d);

          console.log(`🔍 PARSED:`, JSON.stringify(parsed, null, 2));

          resolve(parsed);
        } catch (e) {
          reject(new Error(`JSON parse error: ${d}`));
        }
      });
    });

    req.on("error", reject);

    req.end();
  });
}

// ─────────────────────────────────────────
// TELEGRAM
// ─────────────────────────────────────────
function sendTelegram(msg) {
  const body = JSON.stringify({
    chat_id: CONFIG.TELEGRAM_CHAT_ID,
    text: msg,
    parse_mode: "HTML"
  });

  const options = {
    hostname: "api.telegram.org",
    path: `/bot${CONFIG.TELEGRAM_BOT_TOKEN}/sendMessage`,
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Content-Length": Buffer.byteLength(body)
    }
  };

  const req = https.request(options);

  req.on("error", (e) => {
    console.error("TG error:", e.message);
  });

  req.write(body);

  req.end();
}

// ─────────────────────────────────────────
// FETCH BTC PRICE
// ─────────────────────────────────────────
async function fetchBTCPrice() {
  return new Promise((resolve, reject) => {
    const req = https.request(
      {
        hostname: "fapi.binance.com",
        path: "/fapi/v1/ticker/price?symbol=BTCUSDT",
        method: "GET"
      },
      (res) => {
        let d = "";

        res.on("data", (c) => (d += c));

        res.on("end", () => {
          try {
            const json = JSON.parse(d);

            resolve(parseFloat(json.price));
          } catch (e) {
            reject(e);
          }
        });
      }
    );

    req.on("error", reject);

    req.end();
  });
}

// ─────────────────────────────────────────
// TEST ORDER
// ─────────────────────────────────────────
async function placeTestOrder() {
  console.log("\n🧪 STARTUP TEST ORDER\n");

  try {
    // ───── SET LEVERAGE ─────
    const levRes = await bingxRequest("POST", "/openApi/swap/v2/trade/leverage", {
      symbol: "BTC-USDT",
      side: "SHORT",
      leverage: String(CONFIG.TEST_LEVERAGE)
    });

    if (levRes.code === 0) {
      console.log(`✅ Leverage set to ${CONFIG.TEST_LEVERAGE}x`);
    }

    // ───── PRICE ─────
    const currentPrice = await fetchBTCPrice();

    console.log(`💰 BTC price: ${currentPrice}`);

    // ───── TEST ORDER ─────
    const slPrice = round(currentPrice + CONFIG.TEST_SL_DIST, CONFIG.PRICE_PRECISION);

    const tpPrice = round(currentPrice - CONFIG.TEST_TP_DIST, CONFIG.PRICE_PRECISION);

    console.log(`📋 SELL SHORT ${CONFIG.TEST_QTY} BTC | Entry ~$${currentPrice} | SL $${slPrice} | TP $${tpPrice}`);

    const params = {
      symbol: "BTC-USDT",
      side: "SELL",
      positionSide: "SHORT",
      type: "MARKET",
      quantity: CONFIG.TEST_QTY,

      stopLoss: JSON.stringify({
        type: "STOP_MARKET",
        stopPrice: slPrice,
        price: slPrice,
        workingType: "MARK_PRICE"
      }),

      takeProfit: JSON.stringify({
        type: "TAKE_PROFIT_MARKET",
        stopPrice: tpPrice,
        price: tpPrice,
        workingType: "MARK_PRICE"
      })
    };

    const res = await bingxRequest("POST", "/openApi/swap/v2/trade/order", params);

    if (res.code === 0) {
      const orderId = res.data?.order?.orderId || res.data?.orderId || "N/A";

      console.log(`✅ TEST ORDER SUCCESS`);

      sendTelegram(`🧪 <b>STARTUP TEST ORDER SUCCESS</b>\n\n` + `🔴 SHORT BTC-USDT\n` + `📦 Qty: ${CONFIG.TEST_QTY}\n` + `💰 Entry: ${currentPrice}\n` + `🛑 SL: ${slPrice}\n` + `🎯 TP: ${tpPrice}\n` + `🔖 OrderId: ${orderId}`);
    } else {
      console.error(`❌ TEST ORDER FAILED: ${res.code} ${res.msg}`);

      sendTelegram(`❌ <b>TEST ORDER FAILED</b>\n${res.code} ${res.msg}`);
    }
  } catch (e) {
    console.error(`❌ Test order exception: ${e.message}`);

    sendTelegram(`❌ <b>TEST ORDER EXCEPTION</b>\n${e.message}`);
  }
}

// ─────────────────────────────────────────
// DETECTION LOGIC
// ─────────────────────────────────────────
function detect(candles) {
  if (candles.length < CONFIG.LOOKBACK + 1) {
    return null;
  }

  const prev = candles.slice(-(CONFIG.LOOKBACK + 1), -1);

  const current = candles[candles.length - 1];

  const avgBody = avg(prev.map(bodySize));

  const avgVol = avg(prev.map((c) => c.volume));

  const curBody = bodySize(current);

  const curVol = current.volume;

  if (bodyPct(current) < CONFIG.MIN_BODY_PCT) {
    return null;
  }

  if (curBody < avgBody * CONFIG.BODY_MULTIPLIER) {
    return null;
  }

  if (curVol < avgVol * CONFIG.VOLUME_MULTIPLIER) {
    return null;
  }

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

  if (!isSideways && !isLevelHeld) {
    return null;
  }

  const bullish = isBullish(current) && current.close > topLevel;

  const bearish = isBearish(current) && current.close < botLevel;

  if (!bullish && !bearish) {
    return null;
  }

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
// POSITION SIZE
// ─────────────────────────────────────────
function calcQty(entry, sl) {
  const dist = Math.abs(entry - sl);

  if (!dist) return null;

  const qty = round(CONFIG.RISK_USDT / dist, CONFIG.QTY_PRECISION);

  return qty < CONFIG.MIN_QTY ? CONFIG.MIN_QTY : qty;
}

// ─────────────────────────────────────────
// CHECK TRADE OPEN
// ─────────────────────────────────────────
async function isTradeOpen(symbol) {
  try {
    const res = await bingxRequest("GET", "/openApi/swap/v2/user/positions", {
      symbol
    });

    const positions = res.data || [];

    return positions.some((p) => {
      return p.symbol === symbol && parseFloat(p.positionAmt) !== 0;
    });
  } catch (e) {
    console.error("isTradeOpen:", e.message);

    return false;
  }
}

// ─────────────────────────────────────────
// PLACE ORDER
// ─────────────────────────────────────────
async function placeOrder(signal, bingxSym) {
  const { direction, entry, sl } = signal;

  const qty = calcQty(entry, sl);

  if (!qty) {
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

    stopLoss: JSON.stringify({
      type: "STOP_MARKET",
      stopPrice: slPrice,
      price: slPrice,
      workingType: "MARK_PRICE"
    }),

    takeProfit: JSON.stringify({
      type: "TAKE_PROFIT_MARKET",
      stopPrice: tp,
      price: tp,
      workingType: "MARK_PRICE"
    })
  };

  console.log(`\n📤 LIVE ORDER | ${direction} ${qty} ${bingxSym}`);

  try {
    const res = await bingxRequest("POST", "/openApi/swap/v2/trade/order", params);

    if (res.code === 0) {
      const orderId = res.data?.order?.orderId || res.data?.orderId || "N/A";

      return {
        qty,
        tp,
        slPrice,
        orderId
      };
    }

    console.error(`❌ ORDER FAILED: ${res.code} ${res.msg}`);

    return null;
  } catch (e) {
    console.error(`❌ ORDER ERROR: ${e.message}`);

    return null;
  }
}

// ─────────────────────────────────────────
// HANDLE SIGNAL
// ─────────────────────────────────────────
async function handleSignal(binanceSym, signal) {
  const bingxSym = binanceSym.replace("usdt", "-USDT").toUpperCase();

  const now = Date.now();

  const tradeOpen = await isTradeOpen(bingxSym);

  if (tradeOpen) {
    console.log(`⏭ ${bingxSym} trade already open`);

    return;
  }

  if (now - lastCloseTime[bingxSym] < CONFIG.LAST_CLOSE_COOLDOWN_MS) {
    console.log(`⏭ ${bingxSym} cooldown active`);

    return;
  }

  const order = await placeOrder(signal, bingxSym);

  if (!order) {
    sendTelegram(`❌ <b>ORDER FAILED</b>\n${bingxSym}`);

    return;
  }

  const dir = signal.direction === "BULLISH" ? "🟢 BULLISH" : "🔴 BEARISH";

  const msg = `<b>🚀 TRADE PLACED — ${bingxSym}</b>\n\n` + `${dir} | ${signal.pattern}\n\n` + `💰 Entry: ${signal.entry}\n` + `🛑 SL: ${order.slPrice}\n` + `🎯 TP: ${order.tp}\n` + `📦 Qty: ${order.qty}\n` + `💵 Risk: $${CONFIG.RISK_USDT}\n` + `🔖 OrderId: ${order.orderId}`;

  console.log("\n" + "=".repeat(60));
  console.log(msg.replace(/<[^>]+>/g, ""));
  console.log("=".repeat(60));

  sendTelegram(msg);

  openOrderIds[bingxSym] = order.orderId;

  pollForClose(bingxSym);
}

// ─────────────────────────────────────────
// POLL FOR CLOSE
// ─────────────────────────────────────────
function pollForClose(bingxSym) {
  const interval = setInterval(async () => {
    try {
      const stillOpen = await isTradeOpen(bingxSym);

      if (!stillOpen) {
        console.log(`✅ ${bingxSym} position closed`);

        lastCloseTime[bingxSym] = Date.now();

        openOrderIds[bingxSym] = null;

        clearInterval(interval);

        sendTelegram(`✅ <b>POSITION CLOSED</b>\n${bingxSym}\nCooldown started`);
      }
    } catch (e) {
      console.error("pollForClose:", e.message);
    }
  }, 30000);
}

// ─────────────────────────────────────────
// SEED CANDLES
// ─────────────────────────────────────────
function fetchSeedCandles(symbol) {
  return new Promise((resolve, reject) => {
    const path = `/fapi/v1/klines?symbol=${symbol.toUpperCase()}` + `&interval=${CONFIG.TIMEFRAME}` + `&limit=${CONFIG.LOOKBACK + 1}`;

    const req = https.request(
      {
        hostname: "fapi.binance.com",
        path,
        method: "GET"
      },
      (res) => {
        let d = "";

        res.on("data", (c) => (d += c));

        res.on("end", () => {
          try {
            const rows = JSON.parse(d);

            const closed = rows.slice(0, -1).map((r) => ({
              openTime: r[0],
              open: parseFloat(r[1]),
              high: parseFloat(r[2]),
              low: parseFloat(r[3]),
              close: parseFloat(r[4]),
              volume: parseFloat(r[5])
            }));

            candleStore[symbol].push(...closed);

            console.log(`📥 ${symbol.toUpperCase()} seeded`);

            resolve();
          } catch (e) {
            reject(e);
          }
        });
      }
    );

    req.on("error", reject);

    req.end();
  });
}

// ─────────────────────────────────────────
// START WEBSOCKET
// ─────────────────────────────────────────
async function startWebSocket() {
  const binanceSyms = CONFIG.SYMBOLS.map((s) => s.replace("-", "").toLowerCase());

  await syncServerTime();

  // ───── TEST ORDER ─────
  if (CONFIG.TEST_ORDER) {
    await placeTestOrder();
  }

  await Promise.all(binanceSyms.map(fetchSeedCandles));

  console.log("✅ Seed complete. Connecting...\n");

  const ws = new WebSocket(`wss://fstream.binance.com/stream?streams=${binanceSyms.map((s) => `${s}@kline_${CONFIG.TIMEFRAME}`).join("/")}`);

  ws.on("open", () => {
    console.log("✅ WS connected.");

    sendTelegram(`🤖 <b>Trade Bot Started</b>\n` + `Mode: LIVE\n` + `Symbols: ${CONFIG.SYMBOLS.join(", ")}`);
  });

  ws.on("message", async (raw) => {
    try {
      const msg = JSON.parse(raw);

      const kline = msg.data?.k || msg.k;

      if (!kline || !kline.x) {
        return;
      }

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

      if (candleStore[sym].length > 50) {
        candleStore[sym].shift();
      }

      console.log(`[${sym.toUpperCase()}] ${toIST(candle.openTime)} | Close: ${candle.close}`);

      const signal = detect(candleStore[sym]);

      if (signal) {
        await handleSignal(sym, signal);
      }
    } catch (e) {
      console.error("WS msg:", e.message);
    }
  });

  ws.on("close", () => {
    console.warn("⚠️ WS disconnected. Reconnecting...");

    setTimeout(startWebSocket, 5000);
  });

  ws.on("error", (err) => {
    console.error("WS error:", err.message);

    ws.terminate();
  });
}

// ─────────────────────────────────────────
// VALIDATE ENV
// ─────────────────────────────────────────
const required = ["TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID", "BINGX_API_KEY", "BINGX_API_SECRET"];

const missing = required.filter((k) => !CONFIG[k]);

if (missing.length) {
  console.error(`❌ Missing ENV: ${missing.join(", ")}`);

  process.exit(1);
}

// ─────────────────────────────────────────
// START
// ─────────────────────────────────────────
startWebSocket();
