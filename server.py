"""Fyers trading terminal backend: OAuth, live WebSocket ticks -> browser, REST proxy, paper trading."""
import os, json, threading
from datetime import date, datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent / ".env")  # must run before os.environ[...] is read

from flask import Flask, redirect, request, jsonify, send_from_directory, session
from flask_socketio import SocketIO
from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import data_ws

CLIENT_ID = os.environ["FYERS_CLIENT_ID"]
SECRET = os.environ["FYERS_SECRET_KEY"]
REDIRECT = os.getenv("FYERS_REDIRECT_URI", "")
TRADING = os.getenv("ENABLE_TRADING", "0") == "1"
PAPER = True   # current mode: always starts in paper; switch with POST /api/mode
MAX_LIVE_QTY = int(os.getenv("MAX_LIVE_QTY", "100"))   # per-order cap for live orders
DATA_DIR = os.getenv("DATA_DIR", ".")   # point at a persistent disk when hosted
TOKEN_FILE = os.path.join(DATA_DIR, "token.json")
PAPER_FILE = os.path.join(DATA_DIR, "paper.json")
IST = 19800  # chart timestamps are shifted to IST

HOST = os.getenv("HOST", "127.0.0.1")
PORT = int(os.getenv("PORT", "10000"))
PUBLIC_URL = os.getenv("PUBLIC_URL", "")        # e.g. https://terminal.example.com (set when hosted)
APP_PASSWORD = os.getenv("APP_PASSWORD", "")    # protects the whole site when set

app = Flask(__name__, static_folder="static")
app.secret_key = os.getenv("SECRET_KEY") or os.urandom(24)
app.config.update(SESSION_COOKIE_HTTPONLY=True, SESSION_COOKIE_SAMESITE="Lax",
                  SESSION_COOKIE_SECURE=os.getenv("COOKIE_SECURE", "0") == "1",
                  PERMANENT_SESSION_LIFETIME=timedelta(hours=12))
sio = SocketIO(app, cors_allowed_origins=[PUBLIC_URL] if PUBLIC_URL else None, async_mode="threading")

# ---------- site password ----------
import hmac, time
LOGIN_HTML = """<!doctype html><meta name=viewport content="width=device-width,initial-scale=1"><title>Sign in</title>
<body style="background:#14171c;color:#d8dde6;font:15px system-ui;display:grid;place-items:center;height:100vh;margin:0">
<form method=post style="display:grid;gap:10px;width:260px"><b>Terminal</b>
<input type=password name=pw placeholder="Password" autofocus style="padding:9px;border-radius:4px;border:1px solid #2a303a;background:#1b1f26;color:#d8dde6">
<button style="padding:9px;border:0;border-radius:4px;background:#e3b341;font-weight:600">Sign in</button>{msg}</form>"""


@app.before_request
def gate():
    if not APP_PASSWORD or session.get("ok") or request.path in ("/app-login", "/app-logout") or request.path.startswith("/socket.io"):
        return None
    if request.path.startswith("/api/"):
        return jsonify({"s": "error", "message": "Sign in required"}), 401
    return redirect("/app-login")


@sio.on("connect")
def on_connect(auth=None):
    if APP_PASSWORD and not session.get("ok"):
        return False


@app.route("/app-login", methods=["GET", "POST"])
def app_login():
    if not APP_PASSWORD: return redirect("/")
    msg = ""
    if request.method == "POST":
        if hmac.compare_digest(request.form.get("pw", "").encode(), APP_PASSWORD.encode()):
            session["ok"] = True; session.permanent = True
            return redirect("/")
        time.sleep(1)
        msg = '<span style="color:#e5535b">Wrong password</span>'
    return LOGIN_HTML.replace("{msg}", msg)


@app.route("/app-logout")
def app_logout():
    session.clear()
    return redirect("/app-login")


fyers = None            # REST client
ws = None               # data socket
ws_ready = False
subscribed = set()
lock = threading.Lock()
last_ltp = {}           # symbol -> latest tick price


# ---------- paper trading ----------
def paper_load():
    try:
        return json.load(open(PAPER_FILE))
    except Exception:
        return {"cash": float(os.getenv("PAPER_CASH", "1000000")), "orders": [], "pos": {}, "seq": 0}


book = paper_load()
book.setdefault("brk", [])     # stop-loss / target brackets
book.setdefault("trades", [])  # closed-trade journal


def paper_save():
    json.dump(book, open(PAPER_FILE, "w"))


def paper_fill(o, price):
    q = o["side"] * o["qty"]
    p = book["pos"].setdefault(o["symbol"], {"qty": 0, "avg": 0.0, "realized": 0.0})
    if p["qty"] == 0 or (p["qty"] > 0) == (q > 0):           # opening or adding
        p["avg"] = (p["avg"] * abs(p["qty"]) + price * abs(q)) / (abs(p["qty"]) + abs(q))
        p["qty"] += q
    else:                                                     # reducing or flipping
        closing = min(abs(q), abs(p["qty"]))
        pnl = closing * (price - p["avg"]) * (1 if p["qty"] > 0 else -1)
        p["realized"] += pnl
        book["trades"].append({"symbol": o["symbol"], "dir": "LONG" if p["qty"] > 0 else "SHORT",
                               "qty": closing, "entry": p["avg"], "exit": price, "pnl": pnl,
                               "time": datetime.now().strftime("%d-%b %H:%M:%S"), "note": ""})
        p["qty"] += q
        if p["qty"] == 0:
            p["avg"] = 0.0
        elif (p["qty"] > 0) == (q > 0):                       # flipped sides
            p["avg"] = price
    book["cash"] -= q * price
    o.update(status=2, tradedPrice=price, message="Filled (paper)")
    if o.get("sl") or o.get("tp"):
        book["brk"].append({"symbol": o["symbol"], "dir": o["side"], "qty": o["qty"],
                            "sl": o.get("sl"), "tp": o.get("tp")})


def paper_check(sym, ltp):
    """Fill resting limit orders when a tick crosses their price."""
    with lock:
        hit = False
        for o in book["orders"]:
            if o["status"] == 6 and o["symbol"] == sym:
                if (o["side"] == 1 and ltp <= o["limitPrice"]) or (o["side"] == -1 and ltp >= o["limitPrice"]):
                    paper_fill(o, o["limitPrice"])
                    hit = True
        for br in book["brk"][:]:
            if br["symbol"] != sym:
                continue
            pos = book["pos"].get(sym, {"qty": 0})
            if pos["qty"] * br["dir"] <= 0:          # position already closed or flipped
                book["brk"].remove(br); hit = True
                continue
            px = why = None
            long_ = br["dir"] == 1
            if br.get("sl") and ((long_ and ltp <= br["sl"]) or (not long_ and ltp >= br["sl"])):
                px, why = br["sl"], "Stop-loss"
            elif br.get("tp") and ((long_ and ltp >= br["tp"]) or (not long_ and ltp <= br["tp"])):
                px, why = br["tp"], "Target"
            if px is not None:
                book["seq"] += 1
                q = min(br["qty"], abs(pos["qty"]))
                x = {"id": f"PAPER-{book['seq']}", "orderDateTime": datetime.now().strftime("%H:%M:%S"),
                     "symbol": sym, "side": -br["dir"], "qty": q, "limitPrice": 0, "tradedPrice": 0,
                     "status": 6, "message": ""}
                book["orders"].append(x)
                paper_fill(x, px)
                x["message"] = f"{why} hit (paper)"
                book["brk"].remove(br); hit = True
        if hit:
            paper_save()


def paper_order(b):
    sym = b["symbol"]
    side = 1 if b["side"] == "BUY" else -1
    qty = int(b["qty"])
    lp = float(b.get("price") or 0)
    if qty <= 0:
        return {"s": "error", "message": "Quantity must be at least 1."}
    if b["type"] == "LIMIT" and lp <= 0:
        return {"s": "error", "message": "Enter a limit price."}
    ltp = last_ltp.get(sym)
    if ltp is None:
        try:
            ltp = fyers.quotes({"symbols": sym})["d"][0]["v"]["lp"]
        except Exception:
            return {"s": "error", "message": "No price available for " + sym}
    sl = float(b.get("sl") or 0) or None
    tp = float(b.get("tp") or 0) or None
    ref = lp if b["type"] == "LIMIT" else ltp
    if (sl and ((side == 1 and sl >= ref) or (side == -1 and sl <= ref))) or \
       (tp and ((side == 1 and tp <= ref) or (side == -1 and tp >= ref))):
        return {"s": "error", "message": "For a buy, stop-loss must be below and target above the entry price (reverse for a sell)."}
    with lock:
        book["seq"] += 1
        o = {"id": f"PAPER-{book['seq']}", "orderDateTime": datetime.now().strftime("%H:%M:%S"),
             "symbol": sym, "side": side, "qty": qty, "limitPrice": lp, "tradedPrice": 0,
             "status": 6, "message": "Pending (paper)", "sl": sl, "tp": tp}
        book["orders"].append(o)
        if b["type"] == "MARKET" or (side == 1 and lp >= ltp) or (side == -1 and lp <= ltp):
            paper_fill(o, ltp)
        paper_save()
    return {"s": "ok", "message": f"{o['message']} {o['id']}"}


def paper_portfolio():
    rows = []
    for s, p in book["pos"].items():
        ltp = last_ltp.get(s) or p["avg"]
        rows.append({"symbol": s, "netQty": p["qty"], "netAvg": p["avg"], "ltp": ltp,
                     "pl": p["realized"] + (ltp - p["avg"]) * p["qty"]})
    return {"positions": {"netPositions": rows},
            "orders": {"orderBook": list(reversed(book["orders"]))},
            "funds": {"fund_limit": [{"title": "Paper cash", "equityAmount": book["cash"]},
                                     {"title": "Paper P&L", "equityAmount": sum(r["pl"] for r in rows)}]}}


# ---------- auth ----------
def make_clients(token):
    global fyers
    fyers = fyersModel.FyersModel(client_id=CLIENT_ID, is_async=False, token=token, log_path="")
    start_socket(token)


def load_saved_token():
    try:
        d = json.load(open(TOKEN_FILE))
        if d["date"] == str(date.today()):       # tokens expire daily
            make_clients(d["token"])
    except Exception:
        pass


@app.route("/login")
def login():
    s = fyersModel.SessionModel(client_id=CLIENT_ID, secret_key=SECRET, redirect_uri=REDIRECT,
                                response_type="code", grant_type="authorization_code")
    return redirect(s.generate_authcode())


@app.route("/callback")
def callback():
    s = fyersModel.SessionModel(client_id=CLIENT_ID, secret_key=SECRET, redirect_uri=REDIRECT,
                                response_type="code", grant_type="authorization_code")
    s.set_token(request.args.get("auth_code", ""))
    r = s.generate_token()
    if "access_token" not in r:
        return f"Login failed: {r}", 400
    json.dump({"date": str(date.today()), "token": r["access_token"]}, open(TOKEN_FILE, "w"))
    make_clients(r["access_token"])
    return redirect("/")


# ---------- live data ----------
def start_socket(token):
    global ws, ws_ready

    def on_message(m):
        if isinstance(m, dict) and "symbol" in m and "ltp" in m:
            last_ltp[m["symbol"]] = m["ltp"]
            paper_check(m["symbol"], m["ltp"])   # paper orders keep working in either mode
            sio.emit("tick", m)

    def on_open():
        global ws_ready
        ws_ready = True
        if subscribed:
            ws.subscribe(symbols=list(subscribed), data_type="SymbolUpdate")
        sio.emit("feed", {"live": True})

    def on_close(_):
        global ws_ready
        ws_ready = False
        sio.emit("feed", {"live": False})

    ws = data_ws.FyersDataSocket(access_token=f"{CLIENT_ID}:{token}", log_path="", litemode=False,
                                 write_to_file=False, reconnect=True, on_connect=on_open,
                                 on_close=on_close, on_error=lambda e: print("ws error:", e),
                                 on_message=on_message)
    threading.Thread(target=ws.connect, daemon=True).start()


@sio.on("subscribe")
def on_subscribe(data):
    syms = [s for s in data.get("symbols", []) if s not in subscribed]
    with lock:
        subscribed.update(syms)
    if syms and ws_ready:
        ws.subscribe(symbols=syms, data_type="SymbolUpdate")


@sio.on("unsubscribe")
def on_unsubscribe(data):
    s = data.get("symbol")
    if s in subscribed:
        subscribed.discard(s)
        if ws_ready:
            ws.unsubscribe(symbols=[s], data_type="SymbolUpdate")


# ---------- REST proxy ----------
def need_login():
    return jsonify({"s": "error", "message": "Not logged in"}), 401


@app.route("/api/status")
def status():
    out = {"logged_in": fyers is not None, "live": ws_ready,
           "trading_enabled": TRADING or PAPER, "paper": PAPER,
           "mode": "paper" if PAPER else "live", "live_allowed": TRADING}
    if fyers:
        p = fyers.get_profile()
        out["user"] = p.get("data", {}).get("name")
    return jsonify(out)


@app.route("/api/quotes")
def quotes():
    if not fyers: return need_login()
    return jsonify(fyers.quotes({"symbols": request.args["symbols"]}))


@app.route("/api/history")
def history():
    if not fyers: return need_login()
    days = int(request.args.get("days", 5))
    res = request.args.get("res", "5")
    r = fyers.history({"symbol": request.args["symbol"], "resolution": res, "date_format": "1",
                       "range_from": (date.today() - timedelta(days=days)).isoformat(),
                       "range_to": date.today().isoformat(), "cont_flag": "1"})
    candles = [{"time": c[0] + IST, "open": c[1], "high": c[2], "low": c[3], "close": c[4], "value": c[5]}
               for c in r.get("candles", [])]
    return jsonify({"s": r.get("s"), "candles": candles, "message": r.get("message")})


@app.route("/api/depth")
def depth():
    if not fyers: return need_login()
    return jsonify(fyers.depth({"symbol": request.args["symbol"], "ohlcv_flag": "1"}))


@app.route("/api/portfolio")
def portfolio():
    if not fyers: return need_login()
    if PAPER: return jsonify(paper_portfolio())
    return jsonify({"positions": fyers.positions(), "orders": fyers.orderbook(), "funds": fyers.funds()})


@app.route("/api/order", methods=["POST"])
def order():
    if not fyers: return need_login()
    if PAPER: return jsonify(paper_order(request.json))      # never reaches Fyers
    if not TRADING:
        return jsonify({"s": "error", "message": "Trading disabled. Start with ENABLE_TRADING=1."}), 403
    b = request.json
    if int(b["qty"]) > MAX_LIVE_QTY:
        return jsonify({"s": "error", "message": f"Quantity above the live limit of {MAX_LIVE_QTY}. Raise MAX_LIVE_QTY in .env if intended."}), 400
    typ = {"MARKET": 2, "LIMIT": 1}[b["type"]]
    payload = {"symbol": b["symbol"], "qty": int(b["qty"]), "type": typ,
               "side": 1 if b["side"] == "BUY" else -1, "productType": b.get("product", "INTRADAY"),
               "limitPrice": float(b.get("price") or 0) if typ == 1 else 0, "stopPrice": 0,
               "validity": "DAY", "disclosedQty": 0, "offlineOrder": False}
    return jsonify(fyers.place_order(data=payload))


@app.route("/api/order/<oid>", methods=["DELETE"])
def cancel(oid):
    if not fyers: return need_login()
    if PAPER:
        for o in book["orders"]:
            if o["id"] == oid and o["status"] == 6:
                o.update(status=1, message="Cancelled (paper)")
                paper_save()
                return jsonify({"s": "ok", "message": "Cancelled"})
        return jsonify({"s": "error", "message": "Order not found or already filled."}), 404
    if not TRADING: return jsonify({"s": "error", "message": "Trading disabled."}), 403
    return jsonify(fyers.cancel_order(data={"id": oid}))


UNIVERSE = [f"NSE:{n}-EQ" for n in (
    "RELIANCE TCS HDFCBANK INFY ICICIBANK HINDUNILVR ITC SBIN BHARTIARTL KOTAKBANK LT AXISBANK ASIANPAINT "
    "MARUTI BAJFINANCE HCLTECH SUNPHARMA TITAN ULTRACEMCO WIPRO NESTLEIND ONGC NTPC POWERGRID M&M TATASTEEL "
    "JSWSTEEL ADANIENT ADANIPORTS COALINDIA BAJAJFINSV TECHM INDUSINDBK HINDALCO GRASIM CIPLA DRREDDY "
    "EICHERMOT APOLLOHOSP BRITANNIA HEROMOTOCO DIVISLAB SBILIFE HDFCLIFE TRENT BPCL SHRIRAMFIN BAJAJ-AUTO "
    "TATACONSUM JIOFIN ETERNAL").split()]   # edit freely; any NSE:XXX-EQ symbols work


@app.route("/api/optionchain")
def optionchain():
    if not fyers: return need_login()
    return jsonify(fyers.optionchain(data={"symbol": request.args["symbol"],
                                           "strikecount": int(request.args.get("strikecount", 10)),
                                           "timestamp": request.args.get("timestamp", "")}))


@app.route("/api/optscan")
def optscan():
    """Option chain + live quotes for every strike, so the browser can filter on open/high/low."""
    if not fyers: return need_login()
    r = fyers.optionchain(data={"symbol": request.args["symbol"],
                                "strikecount": max(1, min(int(request.args.get("strikecount", 10)), 25)),
                                "timestamp": request.args.get("timestamp", "")})
    if r.get("s") != "ok": return jsonify(r)
    d = r.get("data", {})
    meta = {x["symbol"]: x for x in d.get("optionsChain", []) if x.get("option_type")}
    syms, rows = list(meta), []
    for i in range(0, len(syms), 50):
        q = fyers.quotes({"symbols": ",".join(syms[i:i + 50])})
        for e in q.get("d", []):
            v, m = e.get("v") or {}, meta.get(e.get("n"))
            if e.get("s") == "ok" and m and v.get("lp") is not None:
                rows.append({"symbol": e["n"], "type": m["option_type"], "strike": m["strike_price"],
                             "open": v.get("open_price"), "high": v.get("high_price"), "low": v.get("low_price"),
                             "ltp": v["lp"], "chp": v.get("chp"), "vol": v.get("volume"),
                             "oi": m.get("oi"), "oich": m.get("oich"), "bid": v.get("bid"), "ask": v.get("ask")})
    return jsonify({"s": "ok", "expiryData": d.get("expiryData", []), "rows": rows})


@app.route("/api/screener")
def screener():
    if not fyers: return need_login()
    syms = [x for x in request.args.get("symbols", "").split(",") if x] or UNIVERSE
    rows = []
    for i in range(0, len(syms), 50):
        r = fyers.quotes({"symbols": ",".join(syms[i:i + 50])})
        for q in r.get("d", []):
            v = q.get("v") or {}
            if q.get("s") == "ok" and v.get("lp") is not None:
                rows.append({"symbol": q["n"], "ltp": v["lp"], "chp": v.get("chp"), "open": v.get("open_price"),
                             "high": v.get("high_price"), "low": v.get("low_price"),
                             "prev": v.get("prev_close_price"), "vol": v.get("volume")})
    return jsonify({"s": "ok", "rows": rows})


# ---------- news + large deals (public sources, not Fyers) ----------
import re, time, csv, io
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
import requests

HDR = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
       "Accept-Language": "en-US,en;q=0.9"}
NEWS_FEEDS = [   # RSS feeds; edit, add or remove freely. Feeds that fail are skipped.
    ("Moneycontrol", "https://www.moneycontrol.com/rss/latestnews.xml"),
    ("Economic Times", "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms"),
    ("Mint", "https://www.livemint.com/rss/markets"),
    ("Business Standard", "https://www.business-standard.com/rss/markets-106.rss"),
]
BULL = set("surge surges soar soars jump jumps rally rallies gain gains rise rises upgrade upgraded buy beat beats profit growth strong bullish breakout wins approval dividend bonus".split())
BEAR = set("fall falls drop drops slump slumps plunge plunges crash decline declines loss losses downgrade downgraded sell miss misses weak bearish fraud probe penalty default cut cuts raid ban warning".split())
ALIASES = {"sbi": "SBIN", "state bank": "SBIN", "hdfc bank": "HDFCBANK", "icici bank": "ICICIBANK", "infosys": "INFY",
           "reliance": "RELIANCE", "tata steel": "TATASTEEL", "airtel": "BHARTIARTL", "kotak": "KOTAKBANK",
           "axis bank": "AXISBANK", "bajaj finance": "BAJFINANCE", "larsen": "LT", "maruti": "MARUTI",
           "sun pharma": "SUNPHARMA", "adani enterprises": "ADANIENT", "adani ports": "ADANIPORTS", "ongc": "ONGC"}
_cache = {}


def cached(key, ttl, fn):
    t, v = _cache.get(key, (0, None))
    if v is None or time.time() - t > ttl:
        v = fn(); _cache[key] = (time.time(), v)
    return v


def fetch_news():
    out, seen = [], set()
    for name, url in NEWS_FEEDS:
        try:
            r = requests.get(url, headers=HDR, timeout=8); r.raise_for_status()
            for it in ET.fromstring(r.content).iter("item"):
                title = (it.findtext("title") or "").strip()
                if not title or title.lower() in seen: continue
                seen.add(title.lower())
                try: ts = parsedate_to_datetime(it.findtext("pubDate")).timestamp()
                except Exception: ts = 0
                out.append({"source": name, "title": title, "link": (it.findtext("link") or "").strip(), "ts": ts,
                            "desc": re.sub(r"<[^>]+>", "", it.findtext("description") or "").strip()[:200]})
        except Exception as e:
            print("news feed failed:", name, e)
    return out


@app.route("/api/news")
def news():
    extra = [x.split(":")[-1][:-3] for x in request.args.get("symbols", "").split(",") if x.endswith("-EQ")]
    names = sorted({u.split(":")[1][:-3] for u in UNIVERSE} | set(extra))
    pats = [(n, re.compile(r"\b" + re.escape(n.lower()) + r"\b")) for n in names if len(n) >= 3]
    items, mood, agg = [], {"bull": 0, "bear": 0, "neutral": 0}, {}
    for it in cached("news", 60, fetch_news):
        text = (it["title"] + " " + it["desc"]).lower()
        words = re.findall(r"[a-z]+", text)
        score = sum(w in BULL for w in words) - sum(w in BEAR for w in words)
        syms = {v for a, v in ALIASES.items() if a in text} | {n for n, rx in pats if rx.search(text)}
        label = "bull" if score > 0 else "bear" if score < 0 else "neutral"
        mood[label] += 1
        for sy in syms:
            a = agg.setdefault(sy, {"symbol": sy, "count": 0, "score": 0}); a["count"] += 1; a["score"] += score
        items.append({**it, "score": score, "label": label, "syms": sorted(syms)})
    items.sort(key=lambda x: -x["ts"])
    top = sorted(agg.values(), key=lambda a: (-a["count"], -abs(a["score"])))[:15]
    return jsonify({"s": "ok", "items": items[:100], "mood": mood, "symbols": top})


def _pick(d, *ks):
    for k in ks:
        if d.get(k) not in (None, ""): return d[k]


def _deal(x):
    return {"date": _pick(x, "date", "BD_DT_DATE", "Date"), "symbol": _pick(x, "symbol", "BD_SYMBOL", "Symbol"),
            "name": _pick(x, "name", "BD_SCRIP_NAME", "Security Name"),
            "client": _pick(x, "clientName", "BD_CLIENT_NAME", "Client Name"),
            "side": _pick(x, "buySell", "BD_BUY_SELL", "Buy/Sell"),
            "qty": _pick(x, "qty", "BD_QTY_TRD", "Quantity Traded"),
            "price": _pick(x, "watp", "BD_TP_WATP", "Trade Price / Wght. Avg. Price")}


def fetch_deals():
    out = {"bulk": [], "block": []}
    try:   # NSE's JSON needs a cookie from the home page first
        ss = requests.Session(); ss.headers.update(HDR)
        ss.get("https://www.nseindia.com", timeout=8)
        j = ss.get("https://www.nseindia.com/api/snapshot-capital-market-largedeal", timeout=10,
                   headers={"Referer": "https://www.nseindia.com/market-data/large-deals"}).json()
        out["bulk"] = [_deal(x) for x in j.get("BULK_DEALS_DATA", [])]
        out["block"] = [_deal(x) for x in j.get("BLOCK_DEALS_DATA", [])]
    except Exception as e:
        print("NSE large-deals JSON failed:", e)
    for key in ("bulk", "block"):   # fallback: NSE's daily CSV files
        if out[key]: continue
        try:
            r = requests.get(f"https://archives.nseindia.com/content/equities/{key}.csv", headers=HDR, timeout=10)
            r.raise_for_status()
            out[key] = [_deal({k.strip(): (v or "").strip() for k, v in row.items() if k})
                        for row in csv.DictReader(io.StringIO(r.text))]
        except Exception as e:
            print(f"NSE {key}.csv failed:", e)
    return out


@app.route("/api/deals")
def deals():
    d = cached("deals", 120, fetch_deals)
    if not d["bulk"] and not d["block"]: _cache.pop("deals", None)   # don't cache an empty result
    note = "" if (d["bulk"] or d["block"]) else "NSE returned no deals. They are published after market close, and NSE sometimes blocks scripts."
    return jsonify({"s": "ok", **d, "note": note})


@app.route("/api/mode", methods=["POST"])
def set_mode():
    global PAPER
    m = (request.json or {}).get("mode")
    if m not in ("paper", "live"):
        return jsonify({"s": "error", "message": "Mode must be paper or live."}), 400
    if m == "live" and not TRADING:
        return jsonify({"s": "error", "message": "Live trading is locked. Set ENABLE_TRADING=1 in .env and restart the server."}), 403
    PAPER = m == "paper"
    return jsonify({"s": "ok", "mode": m, "paper": PAPER, "live_allowed": TRADING})


@app.route("/api/journal")
def journal():
    t = book["trades"]
    pn = [x["pnl"] for x in t]
    wins = [v for v in pn if v > 0]; losses = [v for v in pn if v <= 0]
    daily = {}
    for x in t:
        daily[x["time"][:6]] = daily.get(x["time"][:6], 0) + x["pnl"]
    stats = {"count": len(t), "total": sum(pn), "winrate": 100 * len(wins) / len(t) if t else 0,
             "avgwin": sum(wins) / len(wins) if wins else 0, "avgloss": sum(losses) / len(losses) if losses else 0,
             "best": max(pn) if pn else 0, "worst": min(pn) if pn else 0}
    return jsonify({"paper": True, "stats": stats, "daily": daily,
                    "trades": [dict(x, i=i) for i, x in enumerate(t)][::-1]})


@app.route("/api/journal/note", methods=["POST"])
def journal_note():
    b = request.json
    with lock:
        book["trades"][int(b["i"])]["note"] = str(b.get("note", ""))[:300]
        paper_save()
    return jsonify({"s": "ok"})


@app.route("/")
def index():
    return send_from_directory("static", "index.html")


if __name__ == "__main__":
    if HOST not in ("127.0.0.1", "localhost") and not APP_PASSWORD:
        raise SystemExit("Refusing to start: set APP_PASSWORD before exposing the server to a network.")
    load_saved_token()
    sio.run(app, host=HOST, port=PORT, allow_unsafe_werkzeug=True)
