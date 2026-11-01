"""Polymarket 空窗期錄製器：每次執行記一次快照（每 30 分鐘由排程呼叫）。

記錄兩種資料：
  data/markets-YYYY-MM-DD.csv  市場狀態（UMA 結算狀態、價格、是否已 closed）
  data/books-YYYY-MM-DD.csv    價格釘在高位的市場，領先那一邊的賣方深度（買得到多少量）
state/watch.json               追蹤清單（直到市場結算完成才移除）
"""
import csv, json, os, time
from datetime import datetime, timezone, timedelta
import requests

GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
MIN_VOLUME = 20_000        # 只追蹤成交量夠大的市場
LOOKAHEAD_H = 24           # 預定結束日在未來 24 小時內（或已過）就開始追蹤
PIN_THR = 0.90             # 領先一邊價格 >= 這個值才抓 order book
MAX_WATCH_DAYS = 30        # 追蹤超過這麼久還沒結算就放棄
QUIET_HOURS = 6            # 沒變化的市場，至少隔這麼久才再寫一筆
SLEEP = 0.12
DATA_DIR, STATE_FILE = "data", "state/watch.json"

MARKET_FIELDS = ["id", "conditionId", "question", "endDate", "closed", "active",
                 "umaResolutionStatus", "closedTime", "outcomePrices",
                 "bestBid", "bestAsk", "lastTradePrice", "volume"]
BOOK_FIELDS = ["ts", "id", "question", "token_id", "outcome", "gamma_price",
               "best_bid", "best_bid_size", "best_ask", "best_ask_size",
               "ask_size_le_995", "ask_size_le_999"]


def now():
    return datetime.now(timezone.utc)


def parse_dt(s):
    if not s:
        return None
    s = str(s).replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        try:
            return datetime.fromisoformat(s + ":00")      # 例如 "...+00"
        except ValueError:
            return None


def get(url, params=None):
    for k in range(3):
        try:
            r = requests.get(url, params=params, timeout=30)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (404, 422):
                return None
        except requests.RequestException:
            pass
        time.sleep(1 + k)
    return None


def fnum(x, default=0.0):
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


def find_candidates():
    out, offset, use_order = [], 0, True
    cutoff = now() + timedelta(hours=LOOKAHEAD_H)
    while offset <= 2000:
        params = {"closed": "false", "limit": 100, "offset": offset}
        if use_order:
            params.update({"order": "volumeNum", "ascending": "false"})
        batch = get(f"{GAMMA}/markets", params)
        if batch is None and use_order and offset == 0:
            use_order = False            # 排序參數不被接受，退回預設排序
            continue
        if not batch:
            break
        for m in batch:
            end = parse_dt(m.get("endDate"))
            if end and end <= cutoff and fnum(m.get("volume")) >= MIN_VOLUME:
                out.append(m)
        if use_order and max(fnum(m.get("volume")) for m in batch) < MIN_VOLUME:
            break
        offset += 100
        time.sleep(SLEEP)
    return out


def book_row(ts, m, token, outcome, price):
    b = get(f"{CLOB}/book", {"token_id": token})
    if not b:
        return None
    bids = sorted(b.get("bids", []), key=lambda x: -fnum(x.get("price")))
    asks = sorted(b.get("asks", []), key=lambda x: fnum(x.get("price")))
    row = dict(ts=ts, id=m.get("id"), question=m.get("question"), token_id=token,
               outcome=outcome, gamma_price=price)
    row["best_bid"] = fnum(bids[0]["price"]) if bids else None
    row["best_bid_size"] = fnum(bids[0]["size"]) if bids else None
    row["best_ask"] = fnum(asks[0]["price"]) if asks else None
    row["best_ask_size"] = fnum(asks[0]["size"]) if asks else None
    row["ask_size_le_995"] = sum(fnum(a["size"]) for a in asks if fnum(a["price"]) <= 0.995)
    row["ask_size_le_999"] = sum(fnum(a["size"]) for a in asks if fnum(a["price"]) <= 0.999)
    return row


def append_csv(path, fields, rows):
    if not rows:
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    new = not os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        if new:
            w.writeheader()
        w.writerows(rows)


def load_state():
    try:
        return json.load(open(STATE_FILE, encoding="utf-8"))
    except Exception:
        return {}


def save_state(state):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    json.dump(state, open(STATE_FILE, "w", encoding="utf-8"), ensure_ascii=False, indent=0)


def main():
    t = now()
    ts = t.isoformat(timespec="seconds")
    day = t.strftime("%Y-%m-%d")
    state = load_state()

    for m in find_candidates():
        state.setdefault(str(m["id"]), {"first_seen": ts, "last_sig": None, "last_write": None})

    mrows, brows = [], []
    for mid, info in list(state.items()):
        if (t - parse_dt(info["first_seen"])).days > MAX_WATCH_DAYS:
            del state[mid]
            continue
        m = get(f"{GAMMA}/markets/{mid}")
        time.sleep(SLEEP)
        if not isinstance(m, dict):
            continue                                   # 暫時抓不到，保留追蹤
        try:
            prices = [float(x) for x in json.loads(m.get("outcomePrices") or "[]")]
            toks = json.loads(m.get("clobTokenIds") or "[]")
            outs = json.loads(m.get("outcomes") or "[]")
        except Exception:
            prices, toks, outs = [], [], []

        closed = bool(m.get("closed"))
        uma = m.get("umaResolutionStatus")
        pinned = bool(prices) and max(prices) >= PIN_THR
        sig = f"{closed}|{uma}"
        last_write = parse_dt(info.get("last_write"))
        stale = last_write is None or (t - last_write) >= timedelta(hours=QUIET_HOURS)
        if pinned or sig != info.get("last_sig") or stale:
            row = {k: m.get(k) for k in MARKET_FIELDS}
            row["ts"] = ts
            mrows.append(row)
            info["last_write"] = ts
        info["last_sig"] = sig

        if pinned and not closed and len(toks) == len(prices):
            i = max(range(len(prices)), key=lambda k: prices[k])
            br = book_row(ts, m, toks[i], outs[i] if i < len(outs) else "", prices[i])
            time.sleep(SLEEP)
            if br:
                brows.append(br)

        if closed and (uma in (None, "", "resolved")):
            del state[mid]                              # 已結算，最後一筆已寫入

    append_csv(f"{DATA_DIR}/markets-{day}.csv", ["ts"] + MARKET_FIELDS, mrows)
    append_csv(f"{DATA_DIR}/books-{day}.csv", BOOK_FIELDS, brows)
    save_state(state)
    print(f"{ts} watching={len(state)} market_rows={len(mrows)} book_rows={len(brows)}")


if __name__ == "__main__":
    main()
