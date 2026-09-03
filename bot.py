import os, time, math, logging, threading, traceback
from collections import deque
from datetime import datetime, timezone, timedelta
import ccxt
import pandas as pd
import numpy as np
from flask import Flask, jsonify

# ============================================================
# BINANCE FUTURES | PURE PRICE ACTION + MOMENTUM + ORDER FLOW
# ============================================================
# POOL: GAINERS 1-50 / LOSERS 1-50 / VOLUME 1-50
# BTC/XAU excluded | MAX 3 positions | 10 USDT margin | 2-5x
# LONG/SHORT | DRY_RUN=True | live margin = ISOLATED
#
# 4 ENGINES
# 1) PRICE ACTION
# 2) MOMENTUM
# 3) LIQUIDITY / ORDER FLOW
# 4) POSITION PROTECTION
#
# 3 SIGNAL CLASSES
# NORMAL_CONTINUATION
# EARLY_BREAKOUT
# PRICE_DISCOVERY_EXPANSION
#
# Signal engine uses RAW OHLC only. No EMA/RSI/MACD/ADX/BB/etc.
# ATR is used ONLY for post-entry protection/trailing.
# ============================================================
# FIXES (this revision):
#  - trigger(d1) in pa_engine() was missing the 'side' arg -> crashed every
#    analyze()/monitor() call (TypeError, silently swallowed by outer except).
#  - liq_engine() used "with oblock.acquire():" (bool, not a context manager)
#    -> crashed every call and leaked the lock. Now "with oblock:".
#  - Removed a dead duplicate leverage() definition.
#  - Hardened breach_age against a rare UnboundLocalError.
#  - Added real exchange-side STOP_MARKET reduceOnly protective orders
#    (placed on entry, synced as the trailing stop moves, cancelled on
#    close) so a position stays protected even if this process stalls.
#  - Added a single-line, low-frequency HEARTBEAT log (every
#    POSITION_SUMMARY_INTERVAL sec) instead of adding per-tick log noise.
# ============================================================

DRY_RUN=os.getenv("DRY_RUN","true").lower()=="true"
API_KEY=os.getenv("BINANCE_API_KEY",""); API_SECRET=os.getenv("BINANCE_API_SECRET","")
TF1,TF5,TF15,TF1H,TF4H="1m","5m","15m","1h","4h"
SCAN_INTERVAL=20; MONITOR_INTERVAL=1.0; OHLCV_CACHE_SECONDS=10
POOL_LIMIT=50; MAX_POSITIONS=3; MARGIN=10.0; MIN_LEV=2; MAX_LEV=5
MIN_SCORE=70; ENTRY_SCORE=74
MAX_FUNDING=.0015; COOLDOWN_MIN=60; MIN_QUOTE_VOLUME=2_000_000; MAX_SPREAD=.15
SWING=2; LOOKBACK=70

# Order-flow
OB_LEVELS=20; OB_NEAR=.50; OB_CRITICAL=.25
OB_WALL_MULT=3.0; OB_IMBALANCE_STRONG=1.35; OB_IMBALANCE_WEAK=1.12
OB_HISTORY_SECONDS=4.0; MAX_BOOK_SPREAD=.15

# Position protection: ATR is NOT an entry signal.
ATR_PERIOD=14; INITIAL_STOP_ATR=1.80
BE_TRIGGER=.80; BE_LOCK=.15
# trigger ROI -> locked ROI. Designed to leave runners open.
LOCKS=[(1.2,.35),(2,.80),(3.5,1.6),(5,2.6),(8,4.4),(12,7),(18,11),
       (25,16),(35,23),(50,34),(75,52),(100,72),(150,108)]
TRAIL_BASE=1.55; TRAIL_ACCEL=2.15; TRAIL_WEAK=1.30
TRAIL_MIN_ATR=1.05; TRAIL_MAX_ATR=2.60; STOP_BREACH_CONFIRM_SEC=3.0
MIN_RAW_MOVE=1.00; TARGET_CAP_ATR=6.0; TARGET_MIN_MULT=1.10
MAX_LOSS_TARGET_RATIO=.50; BE_TARGET_RATIO=.40
TIME_WARN=25; TIME_EXIT=90; EMERGENCY_ROI=-.35
POSITIVE_HOLD_SCORE=62; MOMENTUM_HOLD_STATES=("STRONG_ACCELERATING","STRONG","BUILDING")
MAX_HISTORY=1000; REPORT_INTERVAL=5

# Exchange-side protective stop (safety net if the app itself stalls/crashes)
STOP_ORDER_MIN_UPDATE_PCT=0.05  # only replace the resting stop order if it moves >= this % (avoids order spam)

# Low-noise periodic activity summary (does not add per-tick log lines)
POSITION_SUMMARY_INTERVAL=60

logging.basicConfig(level=getattr(logging,os.getenv("LOG_LEVEL","INFO").upper(),logging.INFO),
                    format="%(asctime)s | %(levelname)s | %(message)s")
log=logging.getLogger("PA_ORDERFLOW_BOT")
app=Flask(__name__)
ex=ccxt.binance({"apiKey":API_KEY,"secret":API_SECRET,"enableRateLimit":True,
                 "options":{"defaultType":"future","adjustForTimeDifference":True}})
lock=threading.RLock(); oblock=threading.RLock()
positions={}; cooldowns={}; cache={}; obstate={}; history=[]
last_scan=None; last_report_hour=None; last_heartbeat=0.0; started=datetime.now(timezone.utc).isoformat()
stats={"scans":0,"signals":0,"entries":0,"exits":0,"wins":0,"losses":0,
       "realized_pnl":0.0,"volume":0.0,"trade_seconds":0.0}

def now(): return datetime.now(timezone.utc)
def f(x,d=0.0):
    try:
        x=float(x)
        return d if math.isnan(x) or math.isinf(x) else x
    except: return d
def pct(a,b): return 0 if f(a)==0 else (f(b)-f(a))/f(a)*100
def clamp(x,a,b): return max(a,min(b,x))
def duration(s):
    s=max(0,int(s)); h=s//3600; m=s%3600//60; sec=s%60
    return f"{h}h {m}m {sec}s" if h else f"{m}m {sec}s"
def valid(sym,m=None):
    u=sym.upper()
    return ("/USDT" in u and u not in {"BTC/USDT","XAU/USDT","XAUT/USDT"}
            and not any(x in u for x in ["BTCDOM","UP/","DOWN/","BULL/","BEAR/","_"])
            and (not m or (m.get("active") is not False and m.get("quote")=="USDT"
                           and m.get("settle") in (None,"USDT"))))

def markets():
    try: return ex.load_markets()
    except: log.exception("Market yükleme"); return {}

def tickers():
    out={}; ms=ex.markets or {}
    try: ts=ex.fetch_tickers()
    except: log.exception("Ticker"); return out
    for s,t in ts.items():
        if not valid(s,ms.get(s)): continue
        last=f(t.get("last")); qv=f(t.get("quoteVolume")); bid=f(t.get("bid")); ask=f(t.get("ask"))
        if last<=0 or qv<MIN_QUOTE_VOLUME: continue
        sp=(ask-bid)/((ask+bid)/2)*100 if bid>0 and ask>0 else 0
        if sp>MAX_SPREAD: continue
        out[s]={"symbol":s,"last":last,"percentage":f(t.get("percentage")),
                "quoteVolume":qv,"bid":bid,"ask":ask,"spread":sp}
    return out

def pool(ts):
    a=sorted(ts.values(),key=lambda x:x["percentage"],reverse=True)[:POOL_LIMIT]
    b=sorted(ts.values(),key=lambda x:x["percentage"])[:POOL_LIMIT]
    v=sorted(ts.values(),key=lambda x:x["quoteVolume"],reverse=True)[:POOL_LIMIT]
    p={}
    for name,arr in (("GAINERS",a),("LOSERS",b),("VOLUME",v)):
        for rank,x in enumerate(arr,1):
            p.setdefault(x["symbol"],{"symbol":x["symbol"],"ticker":x,"sources":set(),"ranks":{}})
            p[x["symbol"]]["sources"].add(name); p[x["symbol"]]["ranks"][name]=rank
    return a,b,v,list(p.values())

def priority(c):
    z=len(c["sources"])*20
    for k,r in c["ranks"].items(): z+=max(0,50-r)*(0.45 if k=="VOLUME" else .25)
    return z

def ohlcv(s,tf,n=220):
    k=(s,tf); t=time.time()
    with lock:
        if k in cache and t-cache[k][0]<OHLCV_CACHE_SECONDS: return cache[k][1].copy()
    try:
        x=ex.fetch_ohlcv(s,tf,limit=n)
        if len(x)<40:return None
        d=pd.DataFrame(x,columns=["timestamp","open","high","low","close","volume"])
        for c in ["open","high","low","close","volume"]: d[c]=pd.to_numeric(d[c],errors="coerce")
        d=d.dropna().reset_index(drop=True)
        with lock: cache[k]=(t,d.copy())
        return d
    except: return None

def candle(r):
    o,h,l,c=map(lambda x:f(x),[r.open,r.high,r.low,r.close]); rg=max(h-l,1e-12); body=abs(c-o)
    return {"range":rg,"body":body,"br":body/rg,"cl":(c-l)/rg,
            "uw":max(0,h-max(o,c))/rg,"lw":max(0,min(o,c)-l)/rg,
            "bull":c>o,"bear":c<o}

def swings(d):
    d=d.iloc[-LOOKBACK:].reset_index(drop=True); hs=[]; ls=[]
    for i in range(SWING,len(d)-SWING):
        h=f(d.high[i]); l=f(d.low[i])
        if h>=d.high.iloc[i-SWING:i].max() and h>=d.high.iloc[i+1:i+SWING+1].max(): hs.append((i,h))
        if l<=d.low.iloc[i-SWING:i].min() and l<=d.low.iloc[i+1:i+SWING+1].min(): ls.append((i,l))
    return hs,ls

def structure(d):
    hs,ls=swings(d)
    if len(hs)<2 or len(ls)<2:return "RANGE",hs,ls
    hh=hs[-1][1]>hs[-2][1]; hl=ls[-1][1]>ls[-2][1]
    lh=hs[-1][1]<hs[-2][1]; ll=ls[-1][1]<ls[-2][1]
    return ("BULLISH" if hh and hl else "BEARISH" if lh and ll else "RANGE"),hs,ls

def breakout(d,side,n=20):
    if len(d)<n+2:return False,0
    prev=d.iloc[-n-1:-1]; r=d.iloc[-1]; c=candle(r)
    if side=="LONG":
        level=f(prev.high.max()); return bool(r.close>level and c["br"]>=.45 and c["cl"]>=.60),level
    level=f(prev.low.min()); return bool(r.close<level and c["br"]>=.45 and c["cl"]<=.40),level

def retest(d,side,n=8):
    if len(d)<n+25:return False,0
    old=d.iloc[-n-21:-n]; r=d.iloc[-n:]; c=candle(r.iloc[-1])
    if side=="LONG":
        level=f(old.high.max()); return bool(r.low.min()<=level*1.002 and r.close.iloc[-1]>level
                                            and c["bull"] and c["cl"]>=.60),level
    level=f(old.low.min()); return bool(r.high.max()>=level*.998 and r.close.iloc[-1]<level
                                        and c["bear"] and c["cl"]<=.40),level

def trigger(d,side):
    c=candle(d.iloc[-1]); p=candle(d.iloc[-2])
    if side=="LONG":
        q=(25 if c["bull"] else 0)+(25 if c["br"]>=.55 else 0)+(25 if c["cl"]>=.70 else 0)
        q+=15 if p["bear"] and c["body"]>p["body"] else 0
        return c["bull"] and c["br"]>=.45 and c["cl"]>=.60,q
    q=(25 if c["bear"] else 0)+(25 if c["br"]>=.55 else 0)+(25 if c["cl"]<=.30 else 0)
    q+=15 if p["bull"] and c["body"]>p["body"] else 0
    return c["bear"] and c["br"]>=.45 and c["cl"]<=.40,q

def move_pos(d,side,n=30):
    x=d.iloc[-n:]; hi=f(x.high.max()); lo=f(x.low.min()); p=f(d.close.iloc[-1])
    z=(p-lo)/(hi-lo) if hi>lo else .5
    return z if side=="LONG" else 1-z

# ---------------- REGRESSION / MOVE CAPACITY ENGINE ----------------
def regression_channel(d,n=40):
    if d is None or len(d)<max(20,n):
        return {"slope":0.0,"position":.5,"width":0.0,"upper":0.0,"lower":0.0,"distance_atr":0.0}
    x=d.iloc[-n:].reset_index(drop=True); y=x.close.to_numpy(dtype=float)
    xx=np.arange(len(y),dtype=float)
    slope,inter=np.polyfit(xx,y,1)
    fit=inter+slope*xx
    resid=y-fit
    width=float(np.std(resid))*2.0
    center=float(fit[-1]); upper=center+width; lower=center-width; price=float(y[-1])
    pos=(price-lower)/(upper-lower) if upper>lower else .5
    a=atr(d)
    return {"slope":slope,"position":clamp(pos,0,1),"width":width,"upper":upper,"lower":lower,
            "distance_atr":abs(price-center)/(a or max(price*.001,1e-12))}

def move_capacity(side,d1,d5,d15,d4h):
    """Estimate remaining raw price travel; this is a potential, never a hard TP."""
    r5=regression_channel(d5,40); r15=regression_channel(d15,30); r4=regression_channel(d4h,30)
    a5=atr(d5); a15=atr(d15); price=f(d5.close.iloc[-1])
    if not price:return {"raw_capacity":0.0,"target_raw":0.0,"regression":r5,"valid":False}
    # Channel width + recent expansion + higher-timeframe trend contribution.
    channel_pct=(2*r5["width"]/price*100) if r5["width"]>0 else 0
    recent_pct=abs(dret(d5,5,side))
    accel_bonus=min(2.5,max(0.0,abs(dret(d5,5,side)-dret(d5,10,side))))
    slope15=abs(r15["slope"]*5/price*100) if r15["slope"] else 0
    slope4=abs(r4["slope"]*5/price*100) if r4["slope"] else 0
    atr_pct=(a5/price*100) if a5 else 0
    raw=max(channel_pct*.75, atr_pct*TARGET_CAP_ATR, recent_pct*.80)+accel_bonus*.45+slope15*.35+slope4*.20
    # Avoid pretending a tiny/noisy move can deliver the requested economics.
    raw=max(raw,MIN_RAW_MOVE)
    # Do not count a full channel width if already at its edge.
    pos=r5["position"] if side=="LONG" else 1-r5["position"]
    if pos>.92: raw*=.72
    if pos<.12: raw*=1.08
    return {"raw_capacity":raw,"target_raw":raw,"regression":r5,"regression15":r15,"regression4h":r4,
            "valid":raw>=MIN_RAW_MOVE,"atr_pct":atr_pct,"position":pos}

def target_model(side,d1,d5,d15,d4h,score,state,btc_factor=1.0):
    cap=move_capacity(side,d1,d5,d15,d4h)
    if not cap["valid"]: return {**cap,"target_roi":0.0,"max_loss_roi":0.0}
    strength=1.0 + clamp((score-70)/60,0,.45)
    if state=="STRONG_ACCELERATING": strength+=.15
    elif state=="BUILDING": strength+=.05
    strength*=clamp(btc_factor,.75,1.05)
    raw=min(18.0,max(MIN_RAW_MOVE,cap["target_raw"]*strength))
    return {**cap,"target_raw":raw,"target_roi_base":raw,"target_roi":raw,"max_loss_roi":raw*MAX_LOSS_TARGET_RATIO}

# ---------------- PRICE ACTION ENGINE ----------------
def pa_engine(side,d1,d5,d15,d1h,d4h=None):
    d4h=d4h if d4h is not None else d1h
    s4h,_,_=structure(d4h); s1h,_,_=structure(d1h); s15,_,_=structure(d15); s5,_,_=structure(d5)
    wanted="BULLISH" if side=="LONG" else "BEARISH"
    score=0; rs=[]; ws=[]
    # True 40% PA budget: higher TF structure + 5M structure/breakout + 1M trigger.
    if s4h==wanted: score+=10; rs.append(f"4H {wanted} macro structure")
    elif s4h=="RANGE": score+=5; rs.append("4H range / transition context")
    else: ws.append("4H opposing structure")
    if s1h==wanted: score+=8; rs.append(f"1H {wanted} structure")
    elif s1h=="RANGE": score+=4; rs.append("1H range context")
    else: ws.append("1H opposing structure")
    if s15==wanted: score+=8; rs.append(f"15M {wanted} structure")
    elif s15=="RANGE": score+=4; rs.append("15M range")
    else: ws.append("15M opposing structure")
    b,bl=breakout(d5,side); r,rl=retest(d5,side); tr,tq=trigger(d1,side)
    if b: score+=7; rs.append(f"5M fresh breakout @ {bl:.8g}")
    if r: score+=6; rs.append(f"5M breakout-retest held @ {rl:.8g}")
    if s5==wanted: score+=4; rs.append("5M structure aligned")
    if tr: score+=7; rs.append(f"1M trigger quality {tq:.0f}/100")
    reg=regression_channel(d5,40)
    regpos=reg["position"] if side=="LONG" else 1-reg["position"]
    if reg["slope"]>0 and side=="LONG" or reg["slope"]<0 and side=="SHORT":
        score+=3; rs.append("5M regression slope aligned")
    if .08<=regpos<=.90: score+=2; rs.append("regression channel has room")
    elif regpos>.96: score-=3; ws.append("regression channel extreme")
    return {"score":clamp(score,0,100),"reasons":rs,"warnings":ws,
            "structures":{"1m":structure(d1)[0],"5m":s5,"15m":s15,"1h":s1h,"4h":s4h},
            "breakout":b,"retest":r,"trigger":tr,"trigger_quality":tq,"move_position":move_pos(d5,side),
            "regression":reg,"macro_opposition":s4h not in (wanted,"RANGE")}

# ---------------- MOMENTUM ENGINE: RAW PRICE ----------------
def dret(d,n,side):
    if len(d)<n+1:return 0
    x=pct(d.close.iloc[-n-1],d.close.iloc[-1]); return x if side=="LONG" else -x
def avgbody(d,n=5):
    return float(np.mean([candle(r)["body"] for _,r in d.iloc[-n:].iterrows()])) if len(d) else 0

def momentum_engine(side,d1,d5,d15):
    fast=dret(d5,5,side); slow=dret(d5,15,side); m15=dret(d15,5,side)
    old=0
    if len(d5)>=11:
        x=pct(d5.close.iloc[-11],d5.close.iloc[-6]); old=x if side=="LONG" else -x
    accel=fast-old; prev=avgbody(d5.iloc[:-5],5); be=avgbody(d5,5)/(prev or avgbody(d5,5) or 1)
    seq=sum(1 for _,r in d1.iloc[-5:].iterrows() if (candle(r)["bull"] if side=="LONG" else candle(r)["bear"]))
    score=(20 if fast>0 else 0)+(12 if m15>0 else 0)+(18 if accel>0 else 0)
    score+=18 if be>=1.45 else 10 if be>=1.20 else 0
    score+=15 if seq>=4 else 8 if seq>=3 else 0
    state="STRONG_ACCELERATING" if accel>0 and be>=1.20 else "BUILDING" if accel>0 else "STRONG" if fast>0 else "WEAKENING"
    if fast>0 and accel<0: state="WEAKENING"
    rs=[]; ws=[]
    if fast>0: rs.append(f"5M momentum {fast:+.2f}%")
    if m15>0: rs.append(f"15M momentum {m15:+.2f}%")
    if accel>0: rs.append(f"momentum acceleration {accel:+.2f}%")
    if be>=1.20: rs.append(f"candle expansion x{be:.2f}")
    if seq>=3: rs.append(f"1M directional sequence {seq}/5")
    if state=="WEAKENING": ws.append("momentum weakening")
    return {"score":clamp(score,0,100),"state":state,"acceleration":accel,"body_expansion":be,
            "fast5":fast,"slow5":slow,"reasons":rs,"warnings":ws}

# ---------------- LIQUIDITY / ORDER FLOW ENGINE ----------------
def restbook(s):
    try:
        x=ex.fetch_order_book(s,limit=OB_LEVELS)
        return {"bids":[(f(a[0]),f(a[1])) for a in x["bids"]],"asks":[(f(a[0]),f(a[1])) for a in x["asks"]],"source":"REST"}
    except:return None

def liq_engine(s,side,price):
    with oblock:
        st=obstate.get(s,{}); ob=st.get("book") if time.time()-st.get("ts",0)<2.5 else None
    if ob is None: ob=restbook(s)
    if not ob or not ob["bids"] or not ob["asks"]:
        return {"status":"UNKNOWN","score":0,"risk":50,"reasons":[],"warnings":["order book unavailable"],"metrics":{}}
    bids,asks=ob["bids"],ob["asks"]; bb,ba=bids[0][0],asks[0][0]
    spread=(ba-bb)/((ba+bb)/2)*100
    nb=[x for x in bids if (price-x[0])/price*100<=OB_NEAR]; na=[x for x in asks if (x[0]-price)/price*100<=OB_NEAR]
    bn=sum(p*q for p,q in nb); an=sum(p*q for p,q in na); imb=bn/an if an else 999
    qs=[q for _,q in nb+na]; med=float(np.median(qs)) if qs else 0
    bw=[x for x in nb if med and x[1]>=med*OB_WALL_MULT]; aw=[x for x in na if med and x[1]>=med*OB_WALL_MULT]
    db=min([(price-p)/price*100 for p,_ in bw],default=None); da=min([(p-price)/price*100 for p,_ in aw],default=None)
    score=50; risk=0; rs=[]; ws=[]
    if side=="LONG":
        if imb>=1.35: score+=22; rs.append(f"bid/ask imbalance {imb:.2f}x")
        elif imb>=1.12: score+=12; rs.append(f"bid side stronger {imb:.2f}x")
        elif imb<1/1.35: score-=22; risk+=25; ws.append(f"ask dominance {imb:.2f}x")
        if da is not None:
            risk+=35 if da<=OB_CRITICAL else 18; score-=25 if da<=OB_CRITICAL else 12
            ws.append(f"ask wall {da:.2f}% ahead")
        if db is not None and db<=OB_NEAR: score+=10; rs.append(f"bid support {db:.2f}%")
    else:
        inv=1/imb if 0<imb<999 else 0
        if inv>=1.35: score+=22; rs.append(f"ask/bid imbalance {inv:.2f}x")
        elif inv>=1.12: score+=12; rs.append(f"ask side stronger {inv:.2f}x")
        elif imb>=1.35: score-=22; risk+=25; ws.append(f"bid dominance {imb:.2f}x")
        if db is not None:
            risk+=35 if db<=OB_CRITICAL else 18; score-=25 if db<=OB_CRITICAL else 12
            ws.append(f"bid wall {db:.2f}% below")
        if da is not None and da<=OB_NEAR: score+=10; rs.append(f"ask resistance {da:.2f}%")
    if spread>MAX_BOOK_SPREAD: risk+=30; score-=25; ws.append(f"book spread {spread:.3f}%")
    with oblock:
        q=obstate.setdefault(s,{}).setdefault("hist",deque(maxlen=30))
        q.append({"ts":time.time(),"side":side,"opp":da if side=="LONG" else db})
        recent=[x for x in q if x["ts"]>=time.time()-OB_HISTORY_SECONDS]
    persistent=sum(1 for x in recent if x["opp"] is not None)>=3
    if persistent: risk+=20; score-=15; ws.append("persistent opposing liquidity wall")
    status="BLOCK" if risk>=60 else "WARNING" if risk>=30 else "SAFE"
    return {"status":status,"score":clamp(score,0,100),"risk":clamp(risk,0,100),
            "reasons":rs,"warnings":ws,"metrics":{"imbalance":imb,"spread":spread,"bid_wall":db,"ask_wall":da}}

def ob_monitor():
    while True:
        try:
            with lock: syms=list(positions)
            for s in syms:
                x=restbook(s)
                if x:
                    with oblock: obstate.setdefault(s,{})["book"]=x; obstate[s]["ts"]=time.time()
        except: pass
        time.sleep(1)

# ---------------- FUNDING / EXECUTION ----------------
def funding(s):
    try:return f(ex.fetch_funding_rate(s).get("fundingRate"))
    except:return 0
def cooldown(s):
    with lock:
        u=cooldowns.get(s)
        if not u:return False
        if now()>=u: cooldowns.pop(s,None); return False
        return True
def setcool(s):
    with lock: cooldowns[s]=now()+timedelta(minutes=COOLDOWN_MIN)
def count(): 
    with lock:return len(positions)
def sidecount(side):
    with lock:return sum(1 for p in positions.values() if p["side"]==side)
def qty(s,price,lev):
    try:
        m=ex.market(s); q=MARGIN*lev/price
        if m.get("contract"): q/=f(m.get("contractSize"),1)
        q=f(ex.amount_to_precision(s,q)); mn=f(m.get("limits",{}).get("amount",{}).get("min"))
        return max(q,mn)
    except:return 0
def btc_risk_factor():
    try:
        d=ohlcv("BTC/USDT",TF1H,80)
        if d is None:return 1.0,"BTC neutral"
        m=dret(d,5,"LONG"); a=dret(d,10,"LONG")-dret(d,20,"LONG") if len(d)>=21 else 0
        if abs(m)>=3.0 and a<-.8:return .78,"BTC risk-off"
        if m<=-3.0:return .82,"BTC falling"
        if m>=3.0 and a>=0:return 1.05,"BTC supportive"
        return 1.0,"BTC neutral"
    except:return 1.0,"BTC unavailable"

def analyze(c):
    s=c["symbol"]
    if cooldown(s):return None
    fu=funding(s)
    if abs(fu)>=MAX_FUNDING:return None
    d1,d5,d15,d1h,d4h=[ohlcv(s,x,220) for x in (TF1,TF5,TF15,TF1H,TF4H)]
    if any(x is None for x in (d1,d5,d15,d1h,d4h)):return None
    price=f(c["ticker"]["last"]); results=[]; btc_factor,btc_note=btc_risk_factor()
    ob=restbook(s)
    if ob:
        with oblock: obstate.setdefault(s,{})["book"]=ob; obstate[s]["ts"]=time.time()
    for side in ("LONG","SHORT"):
        pa=pa_engine(side,d1,d5,d15,d1h,d4h); mo=momentum_engine(side,d1,d5,d15); li=liq_engine(s,side,price)
        # 40/30/30: PA / momentum / liquidity. Liquidity is confirmation/risk, not the primary signal.
        score=pa["score"]*.40+mo["score"]*.30+li["score"]*.30
        wanted="BULLISH" if side=="LONG" else "BEARISH"
        structural=pa["structures"]["15m"] in (wanted,"RANGE") or pa["breakout"] or pa["retest"]
        early=pa["breakout"] or pa["retest"] or mo["state"] in ("STRONG_ACCELERATING","BUILDING")
        # Early setups need multiple independent confirmations, but no single indicator is a veto.
        confirmations=sum([pa["trigger"], pa["breakout"], pa["retest"],
                           mo["state"] in ("STRONG_ACCELERATING","BUILDING","STRONG"),
                           li["status"] in ("SAFE","WARNING"),
                           pa["structures"]["4h"] in (wanted,"RANGE")])
        eligible=(score>=MIN_SCORE and structural and early and confirmations>=3 and li["status"]!="BLOCK")
        if pa["breakout"] and mo["state"]=="STRONG_ACCELERATING": cls="PRICE_DISCOVERY_EXPANSION"
        elif (pa["breakout"] or pa["retest"]) and mo["state"] in ("STRONG_ACCELERATING","BUILDING"): cls="EARLY_BREAKOUT"
        else: cls="NORMAL_CONTINUATION"
        target=target_model(side,d1,d5,d15,d4h,score,mo["state"],btc_factor)
        # A trade is rejected only when estimated available raw travel is genuinely below 1%.
        eligible=eligible and target["valid"]
        results.append({"symbol":s,"side":side,"score":score,"price":price,"funding":fu,"pa":pa,"momentum":mo,
                        "liquidity":li,"eligible":eligible,"signal_class":cls,"target":target,
                        "btc_factor":btc_factor,"btc_note":btc_note,"confirmations":confirmations,
                        "sources":sorted(c["sources"]),"ranks":c["ranks"],"ticker_percentage":c["ticker"]["percentage"]})
    results.sort(key=lambda x:x["score"],reverse=True)
    if not results or not results[0]["eligible"] or results[0]["score"]<results[1]["score"]+3:return None
    return results[0]

def decision(sig):
    pa=sig["pa"]; mo=sig["momentum"]; li=sig.get("liquidity_final",sig["liquidity"]); tg=sig["target"]
    return {"signal_class":sig["signal_class"],"side":sig["side"],"score":round(sig["score"],2),
            "structure":pa["structures"],"momentum":mo["state"],"acceleration":round(mo["acceleration"],4),
            "liquidity":li["status"],"liquidity_risk":li["risk"],"confirmations":sig.get("confirmations",0),
            "raw_capacity":round(tg.get("target_raw",0),3),"target_roi":round(tg.get("target_roi",0),2),
            "max_loss_roi":round(tg.get("max_loss_roi",0),2),"btc_factor":sig.get("btc_factor",1.0),
            "reasons":pa["reasons"][:5]+mo["reasons"][:5]+li["reasons"][:3],
            "warnings":pa["warnings"][:2]+mo["warnings"][:2]+li["warnings"][:3]}

def final(sig):
    s=sig["symbol"]
    with lock:
        if s in positions or len(positions)>=MAX_POSITIONS:return False,"position limit"
    if sig["score"]<ENTRY_SCORE:return False,"entry score below threshold"
    if not sig.get("target",{}).get("valid"):return False,"raw move capacity < 1%"
    if sig["liquidity"]["status"]=="BLOCK":return False,"liquidity block"
    li=liq_engine(s,sig["side"],sig["price"])
    if li["status"]=="BLOCK":return False,"order flow changed to BLOCK"
    # WARNING is a risk adjustment, not an automatic veto for an otherwise strong early setup.
    if li["status"]=="WARNING" and sig["score"]<82:return False,"order-flow warning"
    try:
        t=ex.fetch_ticker(s); p=f(t.get("last")); move=abs(pct(sig["price"],p))
        if p<=0 or move>1.50:return False,f"price moved {move:.2f}%"
        sig["price"]=p; sig["liquidity_final"]=li
    except Exception as e:return False,str(e)
    return True,""

def leverage(score,state,btc_factor=1.0):
    base=5 if score>=90 and state=="STRONG_ACCELERATING" else 4 if score>=84 else 3 if score>=78 else 2
    if btc_factor<.85: base-=1
    return int(clamp(base,MIN_LEV,MAX_LEV))

def openpos(sig,lev):
    s=sig["symbol"]; p=sig["price"]; side=sig["side"]; q=qty(s,p,lev)
    if q<=0:return None
    entry=p; order=None
    if not DRY_RUN:
        try:
            ex.set_margin_mode("isolated",s); ex.set_leverage(lev,s)
            order=ex.create_order(s,"market","buy" if side=="LONG" else "sell",q,None,{})
            entry=f(order.get("average"),p)
        except: log.exception("Live entry %s",s); return None
    tg=sig["target"]; target_roi=tg["target_raw"]*lev; max_loss=target_roi*MAX_LOSS_TARGET_RATIO
    pos={"id":f"{s}_{int(time.time()*1000)}","symbol":s,"side":side,"entry_price":entry,"current_price":entry,
         "margin":MARGIN,"leverage":lev,"quantity":q,"opened_at":now().isoformat(),"opened_ts":time.time(),
         "max_roi":0.0,"min_roi":0.0,"peak_price":entry,"peak_ts":time.time(),"stop_price":None,
         "stage":"INITIAL","signal_class":sig["signal_class"],"entry_score":sig["score"],
         "target_raw":tg["target_raw"],"target_roi":target_roi,"max_loss_roi":max_loss,"btc_factor":sig.get("btc_factor",1.0),
         "decision_summary":decision(sig),"last_orderflow_status":sig.get("liquidity_final",sig["liquidity"])["status"],
         "entry_order_id":order.get("id") if order else None,"last_hold_check":0.0,"last_stop_update":0.0,"stop_breach_since":None,
         "stop_order_id":None,"exchange_stop_price":None}
    raw_stop=max_loss/lev
    init_stop=entry*(1-raw_stop/100) if side=="LONG" else entry*(1+raw_stop/100)
    if not DRY_RUN:
        pos["stop_order_id"]=place_stop_order(pos,init_stop); pos["exchange_stop_price"]=init_stop if pos["stop_order_id"] else None
    with lock: positions[s]=pos; stats["signals"]+=1; stats["entries"]+=1; stats["volume"]+=MARGIN*lev
    log.info("ENTRY | %s %s | %.8g | %dx | score %.1f | target %.2f%% | risk <= %.2f%% | %s",
             s,side,entry,lev,sig["score"],target_roi,max_loss,sig["signal_class"])
    log.info("DECISION | %s | confirms=%d | raw-cap %.2f%% | BTC %s", " | ".join(pos["decision_summary"]["reasons"][:7]),sig.get("confirmations",0),tg["target_raw"],sig.get("btc_note","neutral"))
    return pos

# ---------------- POSITION PROTECTION ENGINE ----------------
def place_stop_order(p,stop_price):
    """Place a STOP_MARKET reduceOnly order on the exchange as a hard safety net.
    This is independent of our own polling loop, so the position stays protected
    even if this process stalls, loses connectivity, or crashes."""
    if DRY_RUN or stop_price is None or stop_price<=0: return None
    try:
        side="sell" if p["side"]=="LONG" else "buy"
        order=ex.create_order(p["symbol"],"STOP_MARKET",side,p["quantity"],None,
                               {"stopPrice":stop_price,"reduceOnly":True})
        return order.get("id")
    except Exception:
        log.exception("Stop order placement failed %s",p["symbol"]); return None

def cancel_stop_order(p):
    if DRY_RUN or not p.get("stop_order_id"): return
    try: ex.cancel_order(p["stop_order_id"],p["symbol"])
    except Exception: log.debug("Stop order cancel failed %s",p["symbol"],exc_info=True)

def sync_stop_order(p,stop_price):
    """Replace the resting exchange stop only when it has moved meaningfully,
    to avoid hammering the exchange with cancel/replace calls every tick."""
    if DRY_RUN or stop_price is None or stop_price<=0: return
    prev=p.get("exchange_stop_price")
    if prev and abs(stop_price-prev)/prev*100<STOP_ORDER_MIN_UPDATE_PCT: return
    cancel_stop_order(p)
    oid=place_stop_order(p,stop_price)
    p["stop_order_id"]=oid; p["exchange_stop_price"]=stop_price if oid else prev

def atr(d,n=ATR_PERIOD):
    if d is None or len(d)<n+2:return 0
    pc=d.close.shift(1); tr=pd.concat([d.high-d.low,(d.high-pc).abs(),(d.low-pc).abs()],axis=1).max(axis=1)
    return f(tr.rolling(n).mean().iloc[-1])
def roi(p,price):
    raw=(price-p["entry_price"])/p["entry_price"] if p["side"]=="LONG" else (p["entry_price"]-price)/p["entry_price"]
    return raw*p["leverage"]*100
def pnl(p,price): return roi(p,price),p["margin"]*roi(p,price)/100

def protection(p,price,a,mstate,hold_score=0,structure_ok=True):
    r,_=pnl(p,price); e=p["entry_price"]; side=p["side"]; a=a or abs(price-e)*.02
    target=p.get("target_roi",1.0); max_loss=p.get("max_loss_roi",target*.50)
    # Hard loss ceiling derived from the dynamic target: never allow loss beyond 50% of target ROI.
    raw_stop=max_loss/p["leverage"]
    stop=e*(1-raw_stop/100) if side=="LONG" else e*(1+raw_stop/100)
    stage="RISK"
    # Every 1% ROI advances a non-decreasing profit floor. At 40% of target, move to BE.
    if target>0 and r>=target*BE_TARGET_RATIO:
        be=e
        stop=max(stop,be) if side=="LONG" else min(stop,be); stage="TARGET_40_BE"
        # After BE, trail according to market quality. Strong data gets more breathing room.
        quality=hold_score>=POSITIVE_HOLD_SCORE and mstate in MOMENTUM_HOLD_STATES and structure_ok
        mult=TRAIL_ACCEL if quality and mstate=="STRONG_ACCELERATING" else TRAIL_BASE if quality else TRAIL_WEAK
        mult=clamp(mult,TRAIL_MIN_ATR,TRAIL_MAX_ATR)
        if r>=target*.75: mult=min(TRAIL_MAX_ATR,mult+.25)
        if r>=target: mult=min(TRAIL_MAX_ATR,mult+.25)
        trail=p["peak_price"]-mult*a if side=="LONG" else p["peak_price"]+mult*a
        stop=max(stop,trail) if side=="LONG" else min(stop,trail)
        # Profit lock: one rung for every full 1% ROI, but only tighten when the rung improves.
        whole=int(max(0,r)); lock_roi=max(0.0,whole-0.85)
        lock_price=e*(1+lock_roi/100/p["leverage"]) if side=="LONG" else e*(1-lock_roi/100/p["leverage"])
        stop=max(stop,lock_price) if side=="LONG" else min(stop,lock_price)
        stage += "_RUNNER" if quality else "_TRAIL"
    else:
        # Before BE, use ATR only as a soft volatility buffer while respecting the hard risk ceiling.
        vol=e*(a/e*INITIAL_STOP_ATR*100/p["leverage"]) if e else 0
        atr_stop=e-vol if side=="LONG" else e+vol
        stop=max(stop,atr_stop) if side=="LONG" else min(stop,atr_stop)
        stage="INITIAL_BUFFER"
    age=(time.time()-p["opened_ts"])/60
    # Do not time-exit a healthy runner. Time decay applies only when the thesis is no longer progressing.
    action="HOLD"
    if age>=TIME_EXIT and p["max_roi"]<max(.60,target*.10) and (mstate not in MOMENTUM_HOLD_STATES or not structure_ok): action="TIME_EXIT"
    return stop,stage,action

def close(p,price,reason):
    actual=price
    if not DRY_RUN:
        cancel_stop_order(p)
        try:
            order=ex.create_order(p["symbol"],"market","sell" if p["side"]=="LONG" else "buy",p["quantity"],None,{"reduceOnly":True})
            actual=f(order.get("average"),price)
        except: log.exception("Close %s",p["symbol"]); return
    r,pr=pnl(p,actual); dur=time.time()-p["opened_ts"]
    rec={"id":p["id"],"symbol":p["symbol"],"side":p["side"],"signal_class":p["signal_class"],
         "entry_price":p["entry_price"],"exit_price":actual,"max_roi":p["max_roi"],"close_roi":r,
         "pnl":pr,"duration":duration(dur),"duration_seconds":dur,"closed_at":now().isoformat(),
         "exit_reason":reason,"entry_score":p["entry_score"],"decision_summary":p["decision_summary"]}
    with lock:
        history.append(rec)
        if len(history)>MAX_HISTORY: del history[:-MAX_HISTORY]
        positions.pop(p["symbol"],None); stats["exits"]+=1; stats["realized_pnl"]+=pr; stats["trade_seconds"]+=dur
        stats["wins"]+=pr>=0; stats["losses"]+=pr<0
    setcool(p["symbol"])
    log.info("RESULT | %s %s | ENTRY %.8g | EXIT %.8g | MAX ROI %.2f%% | CLOSE ROI %.2f%% | PNL %.4f | %s",
             p["symbol"],p["side"],p["entry_price"],actual,p["max_roi"],r,pr,duration(dur))
    return rec

def monitor():
    log.info("POSITION PROTECTION ENGINE started")
    while True:
        try:
            with lock: ps=list(positions.values())
            for p in ps:
                try:
                    t=ex.fetch_ticker(p["symbol"]); price=f(t.get("last"))
                    if price<=0:continue
                    d5=ohlcv(p["symbol"],TF5,80); d1=ohlcv(p["symbol"],TF1,80); d15=ohlcv(p["symbol"],TF15,80); d4h=ohlcv(p["symbol"],TF4H,80)
                    mo=momentum_engine(p["side"],d1,d5,d15) if d1 is not None and d5 is not None and d15 is not None else {"state":"NEUTRAL"}
                    r,pr=pnl(p,price); a=atr(d5)
                    with lock:
                        if p["symbol"] not in positions:continue
                        p["current_price"]=price; p["max_roi"]=max(p["max_roi"],r); p["min_roi"]=min(p["min_roi"],r)
                        if (p["side"]=="LONG" and price>p["peak_price"]) or (p["side"]=="SHORT" and price<p["peak_price"]):
                            p["peak_price"]=price; p["peak_ts"]=time.time()
                        hold_score=0
                    if d1 is not None and d5 is not None and d15 is not None:
                        d1h_hold=ohlcv(p["symbol"],TF1H,80)
                        hold_pa=pa_engine(p["side"],d1,d5,d15,d1h_hold if d1h_hold is not None else d15,d4h if d4h is not None else d15)
                        hold_li=liq_engine(p["symbol"],p["side"],price)
                        hold_score=hold_pa["score"]*.55+mo["score"]*.45
                        structure_ok=not hold_pa.get("macro_opposition",False)
                    else:
                        structure_ok=True
                    st,stage,action=protection(p,price,a,mo["state"],hold_score,structure_ok); p["stop_price"]=st; p["stage"]=stage; p["hold_score"]=hold_score
                    if not DRY_RUN: sync_stop_order(p,st)
                    of=liq_engine(p["symbol"],p["side"],price)
                    with lock:
                        if p["symbol"] in positions:p["last_orderflow_status"]=of["status"]
                    # Emergency exit is intentionally hard: adverse order flow + opposite PA trigger.
                    emergency=False
                    if of["status"]=="BLOCK" and r<=EMERGENCY_ROI and d1 is not None and d5 is not None:
                        opp="SHORT" if p["side"]=="LONG" else "LONG"; tr,_=trigger(d1,opp); st5,_,_=structure(d5)
                        emergency=tr and st5==("BEARISH" if p["side"]=="LONG" else "BULLISH")
                    hit=(price<=st if p["side"]=="LONG" else price>=st)
                    loss_cap=r<=-p.get("max_loss_roi",999)
                    positive_hold=(hold_score>=POSITIVE_HOLD_SCORE and mo["state"] in MOMENTUM_HOLD_STATES and structure_ok)
                    if hit:
                        breach_age=0.0
                        with lock:
                            if p["symbol"] in positions:
                                if p.get("stop_breach_since") is None: p["stop_breach_since"]=time.time()
                                breach_age=time.time()-p["stop_breach_since"]
                        if positive_hold and not loss_cap:
                            hit=False
                        elif breach_age<STOP_BREACH_CONFIRM_SEC and not loss_cap:
                            hit=False
                    else:
                        with lock:
                            if p["symbol"] in positions: p["stop_breach_since"]=None
                    if emergency: close(p,price,"ORDERFLOW+PA_EMERGENCY_REVERSAL")
                    elif loss_cap: close(p,price,"MAX_LOSS_50PCT_TARGET")
                    elif action=="TIME_EXIT": close(p,price,"TIME_DECAY")
                    elif hit: close(p,price,f"TRAIL_{stage}")
                except Exception: log.debug("position error",exc_info=True)
            global last_heartbeat
            if time.time()-last_heartbeat>=POSITION_SUMMARY_INTERVAL:
                last_heartbeat=time.time()
                with lock: snap=list(positions.values())
                if snap:
                    parts=[f"{q['symbol']} {q['side']} roi={pnl(q,q['current_price'])[0]:+.2f}% stage={q['stage']}" for q in snap]
                    log.info("HEARTBEAT | %d pos | %s",len(snap)," ; ".join(parts))
                else:
                    log.info("HEARTBEAT | no open positions")
        except: log.exception("monitor")
        time.sleep(MONITOR_INTERVAL)

def report():
    global last_report_hour
    while True:
        try:
            h=now().strftime("%Y-%m-%d-%H")
            if now().minute==0 and h!=last_report_hour:
                last_report_hour=h
                with lock: rec=list(history[-50:]); ps=list(positions.values()); st=dict(stats)
                log.info("HOURLY | closed=%d | wins=%d | losses=%d | pnl=%.4f | total pnl=%.4f",
                         len(rec),sum(x["pnl"]>=0 for x in rec),sum(x["pnl"]<0 for x in rec),
                         sum(x["pnl"] for x in rec),st["realized_pnl"])
                for x in rec[-10:]:
                    log.info("TRADE | %s %s | entry %.8g | exit %.8g | maxROI %.2f%% | closeROI %.2f%% | %s | %s",
                             x["symbol"],x["side"],x["entry_price"],x["exit_price"],x["max_roi"],x["close_roi"],x["duration"],x["exit_reason"])
        except: log.exception("report")
        time.sleep(REPORT_INTERVAL)

def scan():
    global last_scan
    last_scan=now().isoformat()
    with lock: stats["scans"]+=1
    ts=tickers(); a,b,v,cands=pool(ts)
    log.info("SCAN | pool g=%d l=%d v=%d | positions=%d/%d | BTC/XAU excluded",len(a),len(b),len(v),count(),MAX_POSITIONS)
    if count()>=MAX_POSITIONS:return
    signals=[]
    for c in sorted(cands,key=priority,reverse=True):
        try:
            s=analyze(c)
            if s: signals.append(s)
        except: log.debug("analyze %s",c["symbol"],exc_info=True)
    signals.sort(key=lambda x:x["score"],reverse=True)
    if signals:
        top=signals[:3]
        summary=" ; ".join(f"{x['symbol']} {x['side']} {x['score']:.0f} {x['signal_class']} cap={x['target']['target_raw']:.1f}%" for x in top)
        log.info("SIGNALS | %d eligible | %s",len(signals),summary)
    else:
        log.info("SIGNALS | none")
    for s in signals:
        if count()>=MAX_POSITIONS:break
        ok,reason=final(s)
        if not ok:continue
        lev=leverage(s["score"],s["momentum"]["state"],s.get("btc_factor",1.0)); openpos(s,lev)

def bot():
    log.info("BOT START | DRY_RUN=%s | maxpos=%d | margin=%.2f | lev=%d-%d | 4H+40/30/30+dynamic target | 3 signal classes",DRY_RUN,MAX_POSITIONS,MARGIN,MIN_LEV,MAX_LEV)
    markets()
    while True:
        try:scan()
        except:log.exception("scan")
        time.sleep(SCAN_INTERVAL)

@app.route("/")
def home(): return jsonify({"bot":"PA_ORDERFLOW","dry_run":DRY_RUN,"pool":"GAINERS/LOSERS/VOLUME 1-50",
    "engines":["PRICE_ACTION","MOMENTUM","LIQUIDITY_ORDER_FLOW","POSITION_PROTECTION"],
    "signal_classes":["NORMAL_CONTINUATION","EARLY_BREAKOUT","PRICE_DISCOVERY_EXPANSION"]})
@app.route("/status")
def status():
    with lock:
        op=[]
        for p in positions.values():
            r,pr=pnl(p,p["current_price"]); op.append({"symbol":p["symbol"],"side":p["side"],"entry_price":p["entry_price"],
                "current_price":p["current_price"],"roi":r,"pnl":pr,"max_roi":p["max_roi"],
                "duration":duration(time.time()-p["opened_ts"]),"stage":p["stage"],"stop_price":p["stop_price"],
                "orderflow":p["last_orderflow_status"],"target_roi":p.get("target_roi"),"max_loss_roi":p.get("max_loss_roi"),"hold_score":p.get("hold_score",0),"decision_summary":p["decision_summary"]})
        return jsonify({"dry_run":DRY_RUN,"last_scan":last_scan,"positions":op,"stats":dict(stats),"recent_trades":history[-20:]})
@app.route("/trades")
def trades(): 
    with lock:return jsonify(history[-100:])
@app.route("/health")
def health():return jsonify({"status":"ok","time":now().isoformat(),"positions":count(),"dry_run":DRY_RUN})

if __name__=="__main__":
    for target,name in [(monitor,"PositionProtection"),(ob_monitor,"OrderBookMonitor"),(bot,"AnalysisLoop"),(report,"HourlyReport")]:
        threading.Thread(target=target,name=name,daemon=True).start()
    app.run(host="0.0.0.0",port=int(os.getenv("PORT","8080")),threaded=True)
