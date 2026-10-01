#!/usr/bin/env python3
"""
굴뚝 TMS 실시간 측정결과(/rltmMesureResult) 30분 단위 수집 · 가동여부 누적 · 전일 가동시간 리포트

사용법
  python tms_collector.py probe  [--facility 이름] [--stack 1]   # 원본 응답 확인 (최초 1회, 필드명 확인용)
  python tms_collector.py collect                                 # 전 사업장·배출구 1회 수집 (30분마다 스케줄 실행)
  python tms_collector.py status                                  # 최신 가동/정지 현황
  python tms_collector.py report [--date 2026-09-30] [--csv out.csv]  # 일별 가동시간 (기본: 어제)

인증키는 환경변수 DATA_GO_KR_KEY 에 넣어두면 config 에 키를 적지 않아도 됨.
표준 라이브러리만 사용 (추가 설치 불필요).
"""
import argparse
import csv
import json
import os
import sqlite3
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

KST = timezone(timedelta(hours=9))
HERE = os.path.dirname(os.path.abspath(__file__))
TIME_FORMATS = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y%m%d%H%M%S", "%Y%m%d%H%M", "%Y-%m-%dT%H:%M:%S"]
TIME_HINTS = ["dt", "time", "일시", "date"]
FLOW_HINTS = ["flux", "flow", "유량"]  # 자동판정 시 배출유량 필드 탐색용


# ---------------------------------------------------------------- 공통
def load_config(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def db_connect(cfg):
    path = os.environ.get("TMS_DB") or cfg.get("db_path", os.path.join(HERE, "tms.db"))
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    con = sqlite3.connect(path)
    con.execute(
        """CREATE TABLE IF NOT EXISTS readings(
            facility TEXT, stack TEXT, slot_ts TEXT, measured_at TEXT,
            running INTEGER, basis TEXT, raw TEXT, fetched_at TEXT,
            PRIMARY KEY(facility, stack, slot_ts))"""
    )
    return con


def floor30(dt):
    return dt.replace(minute=0 if dt.minute < 30 else 30, second=0, microsecond=0)


def parse_time(s):
    s = str(s).strip()
    for fmt in TIME_FORMATS:
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=KST)
        except ValueError:
            pass
    return None


# ---------------------------------------------------------------- API 호출
def call_api(cfg, area, facility, stack):
    params = {
        "serviceKey": cfg.get("service_key") or os.environ.get("DATA_GO_KR_KEY", ""),
        "areaNm": area,
        "factManageNm": facility,
        "stackCode": stack,
        "type": "json",
    }
    # serviceKey 는 포털에서 받은 인코딩 키(%포함)를 그대로 쓰는 경우가 많아 이중 인코딩 방지
    key = params.pop("serviceKey")
    qs = "serviceKey=" + (key if "%" in key else urllib.parse.quote(key, safe="")) + "&" + urllib.parse.urlencode(params)
    url = cfg["endpoint"] + "?" + qs
    with urllib.request.urlopen(url, timeout=30) as r:
        body = r.read().decode("utf-8", errors="replace")
    try:
        return json.loads(body), body
    except json.JSONDecodeError:
        return None, body  # XML 오류 응답 등


def find_items(node):
    """응답 JSON 에서 레코드(dict) 목록을 찾는다. (response.body.items[.item] 등 구조 차이 흡수)"""
    if isinstance(node, list):
        if node and all(isinstance(x, dict) for x in node):
            return node
        for x in node:
            r = find_items(x)
            if r:
                return r
    elif isinstance(node, dict):
        for k in ("items", "item", "data", "list"):
            if k in node:
                r = find_items(node[k])
                if r:
                    return r
        for v in node.values():
            r = find_items(v)
            if r:
                return r
    return []


# ---------------------------------------------------------------- 가동여부 판정
def to_float(v):
    try:
        return float(str(v).replace(",", "").strip())
    except (ValueError, TypeError):
        return None


def judge_running(rec, rule):
    """return (running 1/0/None, 판정근거)"""
    ops = {
        ">": lambda a, b: a > b, ">=": lambda a, b: a >= b,
        "<": lambda a, b: a < b, "==": lambda a, b: a == b, "!=": lambda a, b: a != b,
    }
    if rule and rule.get("field"):
        field = rule["field"]
        if field not in rec:
            return None, f"필드없음:{field}"
        val = to_float(rec[field])
        if val is None:
            return 0, f"{field}=결측"  # 결측(통신불량/정지)을 정지로 볼지는 rule.null_as 로 조정
        thr = float(rule.get("value", 0))
        ok = ops[rule.get("op", ">")](val, thr)
        return int(ok), f"{field}={val}"
    # 자동: 배출유량 계열 필드 > 0
    for k, v in rec.items():
        if any(h in k.lower() for h in FLOW_HINTS):
            val = to_float(v)
            if val is not None:
                return int(val > 0), f"{k}={val}(auto)"
    return None, "판정필드 없음(probe 후 config 의 run_rule 지정 필요)"


def extract_slot(rec, cfg):
    tf = cfg.get("time_field")
    cands = [tf] if tf else [k for k in rec if any(h in k.lower() for h in TIME_HINTS)]
    for k in cands:
        if k in rec:
            dt = parse_time(rec[k])
            if dt:
                return dt
    return None


# ---------------------------------------------------------------- 명령
def cmd_probe(cfg, args):
    fac = next((f for f in cfg["facilities"] if not args.facility or f["name"] == args.facility), None)
    if not fac:
        sys.exit("config 에 해당 사업장 없음")
    stack = args.stack or (fac.get("stacks") or [1])[0]
    data, raw = call_api(cfg, fac.get("area", ""), fac["name"], stack)
    print(json.dumps(data, ensure_ascii=False, indent=2) if data else raw[:3000])
    items = find_items(data) if data else []
    if items:
        print("\n[필드 목록]", list(items[-1].keys()))
        print("[판정 결과]", judge_running(items[-1], cfg.get("run_rule")))


def cmd_collect(cfg, args):
    con = db_connect(cfg)
    now = datetime.now(KST)
    ok = fail = 0
    for fac in cfg["facilities"]:
        stacks = fac.get("stacks")
        auto = not stacks
        stacks = stacks or list(range(1, int(cfg.get("max_stack", 10)) + 1))
        misses = 0
        for st in stacks:
            try:
                data, raw = call_api(cfg, fac.get("area", ""), fac["name"], st)
                items = find_items(data) if data else []
            except Exception as e:  # 네트워크 오류는 다음 주기에 재시도
                print(f"[ERR] {fac['name']}#{st}: {e}", file=sys.stderr)
                fail += 1
                continue
            if not items:
                misses += 1
                if auto and misses >= 2:
                    break  # 자동탐색: 연속 2개 비면 종료
                continue
            misses = 0
            rec = items[-1]  # 최신 레코드
            mdt = extract_slot(rec, cfg) or now
            slot = floor30(mdt)
            running, basis = judge_running(rec, cfg.get("run_rule"))
            con.execute(
                "INSERT OR REPLACE INTO readings VALUES(?,?,?,?,?,?,?,?)",
                (fac["name"], str(st), slot.isoformat(), mdt.isoformat(), running, basis,
                 json.dumps(rec, ensure_ascii=False), now.isoformat()),
            )
            ok += 1
    con.commit()
    print(f"{now:%Y-%m-%d %H:%M} 수집 성공 {ok}건 / 실패 {fail}건")


def cmd_status(cfg, args):
    con = db_connect(cfg)
    rows = con.execute(
        """SELECT facility, stack, slot_ts, running, basis FROM readings r
           WHERE slot_ts=(SELECT MAX(slot_ts) FROM readings WHERE facility=r.facility AND stack=r.stack)
           ORDER BY facility, stack"""
    ).fetchall()
    label = {1: "발전", 0: "정지", None: "판정불가"}
    print(f"{'사업장':<20}{'배출구':<6}{'기준시각':<22}{'상태':<8}근거")
    for f, s, ts, run, basis in rows:
        print(f"{f:<20}{s:<6}{ts[:16]:<22}{label[run]:<8}{basis}")


def cmd_report(cfg, args):
    con = db_connect(cfg)
    day = args.date or (datetime.now(KST) - timedelta(days=1)).strftime("%Y-%m-%d")
    rows = con.execute(
        """SELECT facility, stack,
                  SUM(running=1), SUM(running=0), SUM(running IS NULL), COUNT(*)
           FROM readings WHERE substr(slot_ts,1,10)=? GROUP BY facility, stack ORDER BY facility, stack""",
        (day,),
    ).fetchall()
    out = []
    print(f"[{day}] 배출구별 가동시간 (30분 슬롯 × 0.5h, 하루 48슬롯 기준)")
    print(f"{'사업장':<20}{'배출구':<6}{'가동h':>7}{'정지h':>7}{'수집슬롯':>9}{'커버리지':>9}")
    for f, s, on, off, na, n in rows:
        on, off, na = on or 0, off or 0, na or 0
        cov = n / 48
        out.append([day, f, s, on * 0.5, off * 0.5, na, n, f"{cov:.0%}"])
        warn = "  ※누락 있음" if n < 48 else ""
        print(f"{f:<20}{s:<6}{on*0.5:>7.1f}{off*0.5:>7.1f}{n:>9}{cov:>9.0%}{warn}")
    if args.csv:
        with open(args.csv, "w", newline="", encoding="utf-8-sig") as fh:
            w = csv.writer(fh)
            w.writerow(["일자", "사업장", "배출구", "가동시간(h)", "정지시간(h)", "판정불가슬롯", "수집슬롯", "커버리지"])
            w.writerows(out)
        print("CSV 저장:", args.csv)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["probe", "collect", "status", "report"])
    ap.add_argument("--config", default=os.path.join(HERE, "config.json"))
    ap.add_argument("--facility")
    ap.add_argument("--stack")
    ap.add_argument("--date")
    ap.add_argument("--csv")
    args = ap.parse_args()
    cfg = load_config(args.config)
    {"probe": cmd_probe, "collect": cmd_collect, "status": cmd_status, "report": cmd_report}[args.cmd](cfg, args)


if __name__ == "__main__":
    main()
