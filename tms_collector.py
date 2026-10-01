#!/usr/bin/env python3
"""
굴뚝 TMS 실시간 측정결과(/rltmMesureResult) 수집 · 발전/정지 판정 누적 · 전일 가동시간 리포트

응답 특성(실제 응답 기준)
  - 측정값 필드(nox/co/tsp/sox/hcl/hf/nh3_mesure_value)는 숫자 문자열이거나 상태 문구
    ("가동중지", "보수중", "미수신", "측정자료확인중" 등) 또는 null 이다.
  - 사업장명으로 조회하면(배출구 미지정) 해당 사업장의 모든 배출구가 한 번에 온다 -> 사업장당 1회 호출.

판정 규칙 (judge_running)
  1) 질소산화물(nox)·일산화탄소(co) 중 하나라도 0 초과 숫자 -> 발전
  2) 상태 문구에 '가동중지' 포함                -> 정지
  3) 그 밖의 문구(미수신/보수중/측정자료확인중) -> 판정불가 (측정기기·통신 상태일 수 있어 단정하지 않음)
  4) 숫자가 전부 0 이하                         -> 정지
  5) 전부 null                                  -> 판정불가

사용법
  python tms_collector.py probe  [--facility 이름] [--stack 1|all]
  python tms_collector.py collect
  python tms_collector.py status
  python tms_collector.py report [--date 2026-09-30] [--csv out.csv]

인증키: 환경변수 DATA_GO_KR_KEY / DB 경로: 환경변수 TMS_DB. 표준 라이브러리만 사용.
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
MEASURE_FIELDS = ["nox_mesure_value", "co_mesure_value"]  # 연소 지표 기준 (먼지 등 다른 항목을 쓰려면 config run_rule.fields)
STOP_KEYWORDS = ["가동중지"]


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


def norm_stack(s):
    return str(s).strip().lstrip("0") or "0"


def name_ok(fac, dbname):
    """API 사업장명이 config 항목(name 을 포함하고 exclude 단어는 포함하지 않음)에 해당하는지.
    예: '에스파워' 로 검색하면 '디에스파워㈜' 도 걸리므로 "exclude": ["디에스파워"] 로 제외한다."""
    dbname = str(dbname)
    return fac["name"] in dbname and not any(x in dbname for x in fac.get("exclude", []))


# ---------------------------------------------------------------- API 호출
def call_api(cfg, area, facility, stack):
    key = cfg.get("service_key") or os.environ.get("DATA_GO_KR_KEY", "")
    params = {"factManageNm": facility, "type": "json"}
    if str(stack).strip().lower() not in ("all", ""):  # all 이면 배출구 조건 없이 전체 배출구 조회
        params["stackCode"] = stack
    if area:
        params["areaNm"] = area
    # 포털 Encoding 키(% 포함)는 그대로, Decoding 키는 인코딩해서 사용
    qs = "serviceKey=" + (key if "%" in key else urllib.parse.quote(key, safe="")) + "&" + urllib.parse.urlencode(params)
    url = cfg["endpoint"] + "?" + qs
    for attempt in range(1, 4):  # 일시적 접속 지연 대비: 최대 3회 (20초 제한, 5초·10초 간격)
        try:
            with urllib.request.urlopen(url, timeout=20) as r:
                body = r.read().decode("utf-8", errors="replace")
            break
        except Exception:
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
    """LIKE 검색으로 다른 사업장이 섞여 와도 이름이 맞는 것만 남기고, 배출구 지정 시 해당 배출구만 남긴다."""
    exact = [r for r in items if str(r.get("fact_manage_nm", "")).strip() == facility]
    pool = exact or [r for r in items if facility in str(r.get("fact_manage_nm", ""))] or items
    if str(stack).strip().lower() == "all":
        return pool
    return [r for r in pool if not str(r.get("stack_code", "")).strip()
            or norm_stack(r.get("stack_code")) == norm_stack(stack)]


# ---------------------------------------------------------------- 가동여부 판정
def to_float(v):
    try:
        s = str(v).replace(",", "").strip()
        return float(s) if s else None
    except (ValueError, TypeError):
        return None


def judge_running(rec, rule):
    """return (running 1/0/None, 판정근거)
    rule(선택): {"fields": [...], "value": 0, "stop_keywords": ["가동중지"]}"""
    rule = rule or {}
    fields = rule.get("fields") or ([rule["field"]] if rule.get("field") else MEASURE_FIELDS)
    thr = float(rule.get("value", 0))
    stop_kw = rule.get("stop_keywords") or STOP_KEYWORDS
    short = lambda f: f.replace("_mesure_value", "")

    nums, texts = {}, {}
    for f in fields:
        v = rec.get(f)
        if v is None or str(v).strip() == "":
            continue
        x = to_float(v)
        if x is not None:
            nums[f] = x
        else:
            texts[f] = str(v).strip()

    on = {f: x for f, x in nums.items() if x > thr}
    if on:
        return 1, ",".join(f"{short(f)}={x:g}" for f, x in on.items())
    joined = " ".join(texts.values()).replace(" ", "")
    if any(k in joined for k in stop_kw):
        return 0, next(t for t in texts.values() if any(k in t.replace(" ", "") for k in stop_kw))
    if texts:
        return None, next(iter(texts.values()))  # 미수신 / 보수중 / 측정자료확인중 등
    if nums:
        return 0, ",".join(f"{short(f)}={x:g}" for f, x in nums.items())
    return None, "측정값 없음(null)"


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
        by.setdefault((f, norm_stack(s)), {})[ts] = r

    def stack_key(s):
        return (0, int(s)) if str(s).isdigit() else (1, str(s))

    result = []
    for fac in cfg["facilities"]:
        name = fac["name"]
        dbfacs = sorted({f for (f, s) in by if name_ok(fac, f)})
        for dbf in dbfacs:
            units = units_of(fac) or [(f"배출구{s}", [s]) for s in sorted({s for (f, s) in by if f == dbf}, key=stack_key)]
            for label, stacks in units:
                series = [by.get((dbf, norm_stack(s)), {}) for s in stacks]
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
                result.append((fac.get("display") if fac.get("display") and len(dbfacs) == 1 else dbf,
                               label, stacks, status))
    return result


def detect_interval(cfg, units):
    """데이터 간격(분). config 의 slot_minutes 가 있으면 우선, 없으면 수집된 시각 간격으로 30/60 자동 판정."""
    if cfg.get("slot_minutes"):
        return int(cfg["slot_minutes"])
    ts = sorted({datetime.fromisoformat(t) for _, _, _, st in units for t in st})
    gaps = [(b - a).total_seconds() / 60 for a, b in zip(ts, ts[1:]) if b > a]
    return 60 if gaps and min(gaps) >= 45 else 30


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
    ok = fail = new_rows = 0
    net_fail = 0  # 연속 접속 실패 횟수 (3회 연속이면 중단: 서버 접속 불가로 판단)
    for fac in cfg["facilities"]:
        want = {norm_stack(s) for s in stacks_to_collect(fac)}  # 비어 있으면 전체 배출구 저장
        try:
            data, raw = call_api(cfg, fac.get("area", ""), fac["name"], "all")  # 사업장당 1회 호출
        except Exception as e:  # 네트워크 오류는 다음 주기에 재시도
            print(f"[ERR] {fac['name']}: {e}", file=sys.stderr)
            fail += 1
            net_fail += 1
            if net_fail >= 3:
                print("[중단] 연속 3회 접속 실패 - 이번 주기 수집을 중단합니다", file=sys.stderr)
                break
            continue
        net_fail = 0
        if data is None:
            print(f"[ERR] {fac['name']}: JSON 아님 -> {raw[:200]}", file=sys.stderr)
            fail += 1
            continue
        recs = [r for r in select_records(find_items(data), fac["name"], "all")
                if not any(x in str(r.get("fact_manage_nm", "")) for x in fac.get("exclude", []))]
        if want:
            recs = [r for r in recs if norm_stack(r.get("stack_code", "")) in want]
        if not recs:
            print(f"[주의] '{fac['name']}': 조회된 데이터 없음 (사업장명/배출구 번호 확인 필요)", file=sys.stderr)
            fail += 1
            continue
        found = {str(r.get("fact_manage_nm", "")).strip() for r in recs}
        if len(found) > 1:
            print(f"[주의] '{fac['name']}' 검색 결과가 여러 사업장: {sorted(found)} -> config 이름을 더 구체적으로 쓰세요",
                  file=sys.stderr)
        missing = want - {norm_stack(r.get("stack_code", "")) for r in recs}
        if missing:
            print(f"[주의] '{fac['name']}': 배출구 {sorted(missing)} 데이터 없음", file=sys.stderr)
        for rec in recs:
            fname = str(rec.get("fact_manage_nm") or fac["name"]).strip()  # API 가 준 실제 사업장명으로 저장
            stack = str(rec.get("stack_code", "")).strip()
            mdt = extract_time(rec, cfg) or now
            slot = floor30(mdt) - shift
            running, basis = judge_running(rec, cfg.get("run_rule"))
            key = (fname, stack, slot.isoformat())
            old = con.execute("SELECT running, basis FROM readings WHERE facility=? AND stack=? AND slot_ts=?",
                              key).fetchone()
            raw_s = json.dumps(rec, ensure_ascii=False, separators=(",", ":"))
            if old is None:  # 새 슬롯만 추가 (같은 값을 다시 받으면 DB 를 건드리지 않아 불필요한 커밋 방지)
                con.execute("INSERT INTO readings VALUES(?,?,?,?,?,?,?,?)",
                            (*key, mdt.isoformat(), running, basis, raw_s, now.isoformat()))
                new_rows += 1
            elif old != (running, basis):  # 같은 슬롯의 값이 정정된 경우에만 갱신
                con.execute("UPDATE readings SET measured_at=?, running=?, basis=?, raw=?, fetched_at=? "
                            "WHERE facility=? AND stack=? AND slot_ts=?",
                            (mdt.isoformat(), running, basis, raw_s, now.isoformat(), *key))
                new_rows += 1
        ok += 1
    con.commit()
    print(f"{now:%Y-%m-%d %H:%M} 수집 성공 {ok}개 사업장 / 실패 {fail}개 / 신규·정정 {new_rows}건")
    if ok == 0 and fail > 0:
        sys.exit(1)  # 전부 실패하면 Actions 에서 빨간 표시


def cmd_status(cfg, args):
    con = db_connect(cfg)
    label = {1: "발전", 0: "정지", None: "판정불가"}
    print(f"{'사업장':<24}{'단위':<10}{'배출구':<8}{'기준시각':<18}상태")
    for dbf, unit, stacks, status in build_unit_status(con, cfg):
        if not status:
            continue
        ts = max(status)
        print(f"{dbf:<24}{unit:<10}{'+'.join(stacks):<8}{ts[:16]:<18}{label[status[ts]]}")


def cmd_report(cfg, args):
    con = db_connect(cfg)
    day = args.date or (datetime.now(KST) - timedelta(days=1)).strftime("%Y-%m-%d")
    units = build_unit_status(con, cfg, day)
    interval = detect_interval(cfg, units)
    per = interval / 60
    expected = 1440 // interval
    out = []
    print(f"[{day}] 단위(호기)별 가동시간 (데이터 간격 {interval}분 × {per:g}h, 하루 {expected}슬롯 기준)")
    print(f"{'사업장':<24}{'단위':<10}{'배출구':<8}{'가동h':>7}{'정지h':>7}{'수집슬롯':>9}{'커버리지':>9}")
    for dbf, unit, stacks, status in units:
        vals = list(status.values())
        on, off, na, n = vals.count(1), vals.count(0), vals.count(None), len(vals)
        cov = n / expected
        out.append([day, dbf, unit, "+".join(stacks), on * per, off * per, na, n, f"{cov:.0%}"])
        warn = "  ※누락/판정불가 있음" if n < expected or na else ""
        print(f"{dbf:<24}{unit:<10}{'+'.join(stacks):<8}{on*per:>7.1f}{off*per:>7.1f}{n:>9}{cov:>9.0%}{warn}")
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
