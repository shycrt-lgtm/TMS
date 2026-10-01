#!/usr/bin/env python3
"""
굴뚝 TMS 실시간 측정결과(/rltmMesureResult) 30분 단위 수집 · 가동여부 누적 · 전일 가동시간 리포트

API 응답에는 가동상태/유량 항목이 없고 오염물질 측정값(먼지·SOx·NOx·HCl·HF·NH3·CO)만 있으므로,
설정한 측정값 중 하나라도 기준값(기본 0) 초과이면 '발전', 전부 0이거나 결측이면 '정지'로 판정한다.

사용법
  python tms_collector.py probe  [--facility 이름] [--stack 1]   # 원본 응답 확인
  python tms_collector.py collect                                 # 전 사업장·배출구 1회 수집 (30분마다 스케줄 실행)
  python tms_collector.py status                                  # 최신 가동/정지 현황
  python tms_collector.py report [--date 2026-09-30] [--csv out.csv]  # 일별 가동시간 (기본: 어제)

인증키는 환경변수 DATA_GO_KR_KEY. DB 경로는 환경변수 TMS_DB 로 변경 가능.
표준 라이브러리만 사용.
"""
import argparse
import csv
import json
import os
import sqlite3
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

KST = timezone(timedelta(hours=9))
HERE = os.path.dirname(os.path.abspath(__file__))
TIME_FORMATS = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y%m%d%H%M%S", "%Y%m%d%H%M", "%Y-%m-%dT%H:%M:%S"]
DEFAULT_TIME_FIELD = "mesure_dt"
DEFAULT_RUN_FIELDS = ["nox_mesure_value", "co_mesure_value"]


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
    key = cfg.get("service_key") or os.environ.get("DATA_GO_KR_KEY", "")
    params = {"factManageNm": facility, "type": "json"}
    if str(stack).strip().lower() not in ("all", ""):  # probe 에서 배출구 칸에 all 입력 시 배출구 조건 없이 조회
        params["stackCode"] = stack
    if area:
        params["areaNm"] = area
    # 포털 Encoding 키(% 포함)는 그대로, Decoding 키는 인코딩해서 사용
    qs = "serviceKey=" + (key if "%" in key else urllib.parse.quote(key, safe="")) + "&" + urllib.parse.urlencode(params)
    url = cfg["endpoint"] + "?" + qs
    last_err = None
    for attempt in range(1, 4):  # 일시적 접속 지연 대비: 최대 3회 (20초 제한, 5초·10초 간격)
        try:
            with urllib.request.urlopen(url, timeout=20) as r:
                body = r.read().decode("utf-8", errors="replace")
            break
        except Exception as e:
            last_err = e
            if attempt == 3:
                raise
            time.sleep(5 * attempt)
    try:
        return json.loads(body), body
    except json.JSONDecodeError:
        return None, body  # XML 오류 응답 등


def find_items(node):
    """응답 JSON 에서 레코드(dict) 목록을 찾는다. 단건(dict)·다건(list)·빈 문자열 모두 처리."""
    if isinstance(node, list):
        if node and all(isinstance(x, dict) for x in node):
            return [x for x in node if "mesure_dt" in x or "stack_code" in x] or node
        for x in node:
            r = find_items(x)
            if r:
                return r
    elif isinstance(node, dict):
        if "mesure_dt" in node or "stack_code" in node:
            return [node]  # 단건 레코드
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


def select_records(items, facility, stack):
    """LIKE 검색으로 다른 사업장/배출구가 섞여 와도 해당 사업장·배출구만 남긴다."""
    exact = [r for r in items if str(r.get("fact_manage_nm", "")).strip() == facility]
    pool = exact or [r for r in items if facility in str(r.get("fact_manage_nm", ""))] or items
    out = []
    for r in pool:
        sc = str(r.get("stack_code", "")).strip().lstrip("0")
        if sc and str(stack).strip().lower() != "all" and sc != str(stack).strip().lstrip("0"):
            continue
        out.append(r)
    return out


# ---------------------------------------------------------------- 가동여부 판정
def to_float(v):
    try:
        s = str(v).replace(",", "").strip()
        return float(s) if s else None
    except (ValueError, TypeError):
        return None


def judge_running(rec, rule):
    """return (running 1/0/None, 판정근거)
    rule: {"fields": [...], "value": 0, "null_as": "stop"|"unknown"}
    측정값 중 하나라도 value 초과 → 발전(1). 전부 value 이하 → 정지(0). 전부 결측 → null_as 에 따름."""
    rule = rule or {}
    fields = rule.get("fields") or ([rule["field"]] if rule.get("field") else DEFAULT_RUN_FIELDS)
    thr = float(rule.get("value", 0))
    present = {f: to_float(rec.get(f)) for f in fields}
    present = {f: v for f, v in present.items() if v is not None}
    if not present:
        if rule.get("null_as") == "unknown":
            return None, "측정값 결측"
        return 0, "측정값 전부 결측"
    on = any(v > thr for v in present.values())
    return int(on), ",".join(f"{f.replace('_mesure_value', '')}={v:g}" for f, v in present.items())


def extract_time(rec, cfg):
    tf = cfg.get("time_field") or DEFAULT_TIME_FIELD
    return parse_time(rec[tf]) if tf in rec else None


# ---------------------------------------------------------------- 발전 단위(호기) 구성
def units_of(fac):
    """사업장의 발전 단위 목록 [(라벨, [배출구...])].
    config 에 "units" 가 있으면 그대로 사용 (여러 배출구를 한 단위로 합산 가능),
    없으면 "stacks" 의 각 배출구를 별개 단위로 취급한다."""
    if fac.get("units"):
        return [(u.get("label") or "+".join(str(s) for s in u["stacks"]), [str(s) for s in u["stacks"]])
                for u in fac["units"]]
    return [(f"배출구{s}", [str(s)]) for s in (fac.get("stacks") or [])]


def stacks_to_collect(fac):
    out = []
    for _, ss in units_of(fac):
        for s in ss:
            if s not in out:
                out.append(s)
    return out


def build_unit_status(con, cfg, day=None):
    """배출구별 판정을 단위(호기) 단위로 합친다.
    단위 상태: 소속 배출구 중 하나라도 발전(1) → 발전 / 전부 정지(0) → 정지 / 그 외(결측 포함) → 판정불가.
    return [(DB 사업장명, 단위라벨, [배출구], {slot_ts: 1|0|None})]"""
    sql = "SELECT facility, stack, slot_ts, running FROM readings"
    rows = con.execute(sql + (" WHERE substr(slot_ts,1,10)=?" if day else ""), (day,) if day else ()).fetchall()
    by = {}
    for f, s, ts, r in rows:
        by.setdefault((f, s), {})[ts] = r

    def stack_key(s):
        return (0, int(s)) if str(s).isdigit() else (1, str(s))

    result = []
    for fac in cfg["facilities"]:
        name = fac["name"]
        dbfacs = sorted({f for (f, s) in by if f == name or name in f})
        for dbf in dbfacs:
            units = units_of(fac) or [(f"배출구{s}", [s]) for s in sorted({s for (f, s) in by if f == dbf}, key=stack_key)]
            for label, stacks in units:
                series = [by.get((dbf, s), {}) for s in stacks]
                slots = sorted(set().union(*[set(x) for x in series]))
                status = {}
                for ts in slots:
                    vals = [x.get(ts, "missing") for x in series]
                    if any(v == 1 for v in vals):
                        status[ts] = 1
                    elif all(v == 0 for v in vals):
                        status[ts] = 0
                    else:
                        status[ts] = None
                result.append((dbf, label, stacks, status))
    return result


# ---------------------------------------------------------------- 명령
def cmd_probe(cfg, args):
    fac = next((f for f in cfg["facilities"] if not args.facility or f["name"] == args.facility), None)
    if not fac and args.facility:
        fac = {"name": args.facility, "area": ""}  # config 에 없어도 입력한 이름으로 바로 조회
    if not fac:
        sys.exit("조회할 사업장 이름이 없음")
    stack = args.stack or (stacks_to_collect(fac) or [1])[0]
    data, raw = call_api(cfg, fac.get("area", ""), fac["name"], stack)
    print(json.dumps(data, ensure_ascii=False, indent=2) if data else raw[:3000])
    if data:
        names = sorted({(str(r.get("area_nm", "")).strip(), str(r.get("fact_manage_nm", "")).strip(),
                         str(r.get("stack_code", "")).strip()) for r in find_items(data)})
        print("\n[조회된 사업장 (지역, 사업장명, 배출구)]", names if names else "없음 (이름이 API 의 사업장명과 다르거나 해당 배출구 없음)")
    items = select_records(find_items(data), fac["name"], stack) if data else []
    if items:
        print("\n[레코드 수]", len(items))
        print("[필드 목록]", list(items[-1].keys()))
        for r in items[-3:]:
            print("[판정]", r.get(cfg.get("time_field") or DEFAULT_TIME_FIELD), judge_running(r, cfg.get("run_rule")))


def cmd_collect(cfg, args):
    con = db_connect(cfg)
    now = datetime.now(KST)
    shift = timedelta(minutes=30) if cfg.get("label_is_end") else timedelta(0)
    ok = fail = 0
    net_fail = 0  # 연속 접속 실패 횟수 (3회 연속이면 중단: 서버 접속 불가로 판단)
    abort = False
    for fac in cfg["facilities"]:
        if abort:
            break
        stacks = stacks_to_collect(fac)
        auto = not stacks
        stacks = stacks or list(range(1, int(cfg.get("max_stack", 10)) + 1))
        misses = 0
        for st in stacks:
            try:
                data, raw = call_api(cfg, fac.get("area", ""), fac["name"], st)
            except Exception as e:  # 네트워크 오류는 다음 주기에 재시도
                print(f"[ERR] {fac['name']}#{st}: {e}", file=sys.stderr)
                fail += 1
                net_fail += 1
                if net_fail >= 3:
                    print("[중단] 연속 3회 접속 실패 - 이번 주기 수집을 중단합니다", file=sys.stderr)
                    abort = True
                    break
                continue
            net_fail = 0
            if data is None:
                print(f"[ERR] {fac['name']}#{st}: JSON 아님 -> {raw[:200]}", file=sys.stderr)
                fail += 1
                continue
            recs = select_records(find_items(data), fac["name"], st)
            if not recs:
                misses += 1
                if auto and misses >= 2:
                    break  # 자동탐색: 연속 2개 비면 종료
                continue
            misses = 0
            found = {str(r.get("fact_manage_nm", "")).strip() for r in recs}
            if len(found) > 1 or (found and fac["name"] not in found):
                print(f"[주의] '{fac['name']}' 검색 결과 사업장명: {sorted(found)} -> config 이름을 정확히 맞추세요",
                      file=sys.stderr)
            for rec in recs:  # 여러 건이 오면 모두 저장 (누락 구간 보충)
                fname = str(rec.get("fact_manage_nm") or fac["name"]).strip()  # API 가 준 실제 사업장명으로 저장
                mdt = extract_time(rec, cfg) or now
                slot = floor30(mdt) - shift
                running, basis = judge_running(rec, cfg.get("run_rule"))
                con.execute(
                    "INSERT OR REPLACE INTO readings VALUES(?,?,?,?,?,?,?,?)",
                    (fname, str(st), slot.isoformat(), mdt.isoformat(), running, basis,
                     json.dumps(rec, ensure_ascii=False), now.isoformat()),
                )
            ok += 1
    con.commit()
    print(f"{now:%Y-%m-%d %H:%M} 수집 성공 {ok}건 / 실패 {fail}건")
    if ok == 0 and fail > 0:
        sys.exit(1)  # 전부 실패하면 Actions 에서 빨간 표시


def cmd_status(cfg, args):
    con = db_connect(cfg)
    label = {1: "발전", 0: "정지", None: "판정불가"}
    print(f"{'사업장':<20}{'단위':<10}{'배출구':<8}{'기준시각':<18}상태")
    for dbf, unit, stacks, status in build_unit_status(con, cfg):
        if not status:
            continue
        ts = max(status)
        print(f"{dbf:<20}{unit:<10}{'+'.join(stacks):<8}{ts[:16]:<18}{label[status[ts]]}")


def cmd_report(cfg, args):
    con = db_connect(cfg)
    day = args.date or (datetime.now(KST) - timedelta(days=1)).strftime("%Y-%m-%d")
    out = []
    print(f"[{day}] 단위(호기)별 가동시간 (30분 슬롯 × 0.5h, 하루 48슬롯 기준)")
    print(f"{'사업장':<20}{'단위':<10}{'배출구':<8}{'가동h':>7}{'정지h':>7}{'수집슬롯':>9}{'커버리지':>9}")
    for dbf, unit, stacks, status in build_unit_status(con, cfg, day):
        vals = list(status.values())
        on, off, na, n = vals.count(1), vals.count(0), vals.count(None), len(vals)
        cov = n / 48
        out.append([day, dbf, unit, "+".join(stacks), on * 0.5, off * 0.5, na, n, f"{cov:.0%}"])
        warn = "  ※누락 있음" if n < 48 or na else ""
        print(f"{dbf:<20}{unit:<10}{'+'.join(stacks):<8}{on*0.5:>7.1f}{off*0.5:>7.1f}{n:>9}{cov:>9.0%}{warn}")
    if args.csv:
        with open(args.csv, "w", newline="", encoding="utf-8-sig") as fh:
            w = csv.writer(fh)
            w.writerow(["일자", "사업장", "단위", "배출구", "가동시간(h)", "정지시간(h)", "판정불가슬롯", "수집슬롯", "커버리지"])
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
