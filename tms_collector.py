#!/usr/bin/env python3
"""
굴뚝 TMS 측정결과 수집 · 발전/정지 판정 누적 · 전일 가동시간 리포트
(한국환경공단 CleanSYS 공개 화면 https://cleansys.or.kr 이 내부적으로 쓰는 조회 호출 기반)

사용하는 호출 (모두 POST, form 전달, 인증키 불필요)
  /selectOdaOpenDay.do        factCode, stackCode, itemCd -> {"result":[48건]}  최근 24시간 30분 단위
        행: open_dt("MM-DD HH:MM", 연도 없음), nox/co/tsp/sox/hcl/hf/nh3_mesure_value (숫자 문자열 또는 null)
        ※ 가동중지·미수신 구간은 상태 문구가 아니라 null 로 온다 (itemCd 는 value 필드만 바꾸므로 3 고정)
  /selectOdaOpenFactDetail.do factCode -> 사업장 대표 배출구 1개의 '최신 슬롯' 상태
        result.nox_mesure_value 에 숫자 또는 "측정자료확인중(가동중지)" 같은 상태 문구,
        stackList 에 사업장의 배출구 목록 (배출구 지정은 불가, 항상 대표 배출구만 응답)

판정 규칙 (judge_running)
  1) 질소산화물(nox)·일산화탄소(co) 중 하나라도 0 초과 숫자 -> 발전
  2) 상태 문구에 '가동중지' 포함                -> 정지
  3) 그 밖의 문구(미수신/보수중/측정자료확인중) -> 판정불가
  4) 숫자가 전부 0 이하                         -> 정지
  5) 전부 null                                  -> config "null_as" 에 따름
        "stop"(기본): 정지(basis 'null(상태문구없음)') / "unknown": 판정불가
     - 대표 배출구의 최신 슬롯은 FactDetail 의 상태 문구로 보강하므로 그 문구가 우선한다.
     - 이후 주기에 같은 슬롯이 null 만으로 다시 오더라도 기존 판정을 덮어쓰지 않는다
       (null 은 '정보 없음' 이므로 더 구체적인 기록을 지우지 않음. 숫자로 정정되는 경우만 갱신).

사용법
  python tms_collector.py check                                # 접속·배출구 구성 점검 (DB 건드리지 않음)
  python tms_collector.py probe  [--facility 이름] [--stack 7] # 원자료 확인
  python tms_collector.py collect
  python tms_collector.py status
  python tms_collector.py report [--date 2026-09-30] [--csv out.csv]
  python tms_collector.py latest [--json latest.json]
  python tms_collector.py slots  [--dir reports] [--date 2026-09-30]   # 화면 그래프용 30분 슬롯 파일

DB 경로: 환경변수 TMS_DB. 표준 라이브러리만 사용.
"""
import argparse
import csv
import json
import os
import sqlite3
import ssl
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

KST = timezone(timedelta(hours=9))
HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_BASE = "https://cleansys.or.kr"
MEASURE_FIELDS = ["nox_mesure_value", "co_mesure_value"]  # 연소 지표 기준 (먼지 등 다른 항목을 쓰려면 config run_rule.fields)
STOP_KEYWORDS = ["가동중지"]
NULL_BASIS = "측정값 없음(null)"
NULL_STOP_BASIS = "null(상태문구없음)"
REQUEST_GAP_SEC = 0.3  # 공개 사이트 부담을 줄이기 위한 호출 간격


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


def norm_stack(s):
    return str(s).strip().lstrip("0") or "0"


def name_ok(fac, dbname):
    """DB 에 저장된 사업장명이 config 항목(name 을 포함하고 exclude 단어는 포함하지 않음)에 해당하는지.
    예: '에스파워' 로 검색하면 '디에스파워㈜' 도 걸리므로 "exclude": ["디에스파워"] 로 제외한다."""
    dbname = str(dbname)
    return fac["name"] in dbname and not any(x in dbname for x in fac.get("exclude", []))


# ---------------------------------------------------------------- CleanSYS 화면 호출
def http_post(cfg, path, form):
    """CleanSYS 조회 호출 (POST form). 일시적 접속 지연 대비 최대 3회 (20초 제한, 5초·10초 간격).
    return (JSON 또는 None, 원문)"""
    base = cfg.get("base_url", DEFAULT_BASE).rstrip("/")
    url = base + path
    data = urllib.parse.urlencode(form).encode("utf-8")
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; TMS-collector)",
        "Referer": base + "/index.do",
        "X-Requested-With": "XMLHttpRequest",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    }
    body = ""
    ctx = None
    if cfg.get("verify_ssl", True) is False:
        # CleanSYS 는 중간 인증서를 누락해 보내므로 curl/Python 에서 검증이 실패한다(브라우저는 보완해서 통과).
        # 로그인·개인정보 없는 공개 자료 조회이므로 이 호출에 한해 검증을 생략한다.
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    for attempt in range(1, 4):
        try:
            req = urllib.request.Request(url, data=data, headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=20, context=ctx) as r:
                body = r.read().decode("utf-8", errors="replace")
            break
        except Exception:
            if attempt == 3:
                raise
            time.sleep(5 * attempt)
    try:
        return json.loads(body), body
    except json.JSONDecodeError:
        return None, body  # HTML 오류 페이지 등


def fetch_day(cfg, fact_code, stack):
    """최근 24시간 30분 단위 행 목록 (오래된 순). 실패 시 None."""
    data, _ = http_post(cfg, "/selectOdaOpenDay.do",
                        {"factCode": str(fact_code), "stackCode": str(stack), "itemCd": "3"})
    if not isinstance(data, dict) or not isinstance(data.get("result"), list):
        return None
    return data["result"]


def fetch_detail(cfg, fact_code):
    """사업장 대표 배출구의 최신 슬롯 상태 + 배출구 목록. 실패 시 None."""
    data, _ = http_post(cfg, "/selectOdaOpenFactDetail.do", {"factCode": str(fact_code)})
    if not isinstance(data, dict) or not isinstance(data.get("result"), dict):
        return None
    return data


def parse_open_dt(s, now):
    """'MM-DD HH:MM'(연도 없음) -> KST datetime. 현재보다 하루 넘게 미래면 전년도로 본다(연초 경계)."""
    try:
        t = datetime.strptime(str(s).strip(), "%m-%d %H:%M")
    except ValueError:
        return None
    for year in (now.year, now.year - 1):
        try:
            cand = datetime(year, t.month, t.day, t.hour, t.minute, tzinfo=KST)
        except ValueError:  # 2/29 등
            continue
        if cand <= now + timedelta(days=1):
            return cand
    return None


def parse_detail_dt(s):
    try:
        return datetime.strptime(str(s).strip(), "%Y-%m-%d %H:%M").replace(tzinfo=KST)
    except ValueError:
        return None


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
    return None, NULL_BASIS


def judge_slot(cfg, rec):
    """judge_running + 전부 null 인 슬롯의 처리(config null_as)."""
    running, basis = judge_running(rec, cfg.get("run_rule"))
    if running is None and basis == NULL_BASIS and cfg.get("null_as", "stop") == "stop":
        return 0, NULL_STOP_BASIS
    return running, basis


def is_null_basis(basis):
    return basis in (NULL_BASIS, NULL_STOP_BASIS)


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
def find_facility(cfg, name):
    return next((f for f in cfg["facilities"] if not name or f["name"] == name or f.get("display") == name), None)


def cmd_check(cfg, args):
    """접속 가능 여부와 배출구 구성을 점검한다. DB 를 건드리지 않는다. 하나라도 실패하면 종료코드 1."""
    bad = 0
    print(f"{'사업장':<26}{'대표배출구':<10}{'최신슬롯':<18}{'상태':<26}배출구목록")
    for fac in cfg["facilities"]:
        label = fac.get("display") or fac["name"]
        try:
            d = fetch_detail(cfg, fac["fact_code"])
        except Exception as e:
            print(f"{label:<26}접속 실패: {e}")
            bad += 1
            time.sleep(REQUEST_GAP_SEC)
            continue
        if d is None:
            print(f"{label:<26}응답이 JSON 이 아니거나 형식이 다름 (차단 또는 사이트 변경 가능성)")
            bad += 1
            time.sleep(REQUEST_GAP_SEC)
            continue
        res = d["result"]
        stacks = [str(s.get("stack_code")) for s in d.get("stackList", [])]
        want = stacks_to_collect(fac)
        miss = [s for s in want if s not in stacks]
        note = f"  ※ config 배출구 {miss} 가 사이트 목록에 없음" if miss else ""
        if miss:
            bad += 1
        print(f"{label:<26}{str(res.get('stack_code')):<10}{str(res.get('mesure_dt')):<18}"
              f"{str(res.get('nox_mesure_value')):<26}{','.join(stacks)}{note}")
        time.sleep(REQUEST_GAP_SEC)
    first = cfg["facilities"][0]
    try:
        rows = fetch_day(cfg, first["fact_code"], stacks_to_collect(first)[0])
        print(f"\n[24시간 호출 점검] {first['name']} -> {'실패' if rows is None else str(len(rows)) + '건'}")
        if rows is None:
            bad += 1
    except Exception as e:
        print(f"\n[24시간 호출 점검] 접속 실패: {e}")
        bad += 1
    print("점검 결과:", "이상 없음" if not bad else f"문제 {bad}건")
    if bad:
        sys.exit(1)


def cmd_probe(cfg, args):
    fac = find_facility(cfg, args.facility)
    if not fac:
        sys.exit("config 에 없는 사업장 이름: " + str(args.facility))
    stack = args.stack or (stacks_to_collect(fac) or ["1"])[0]
    now = datetime.now(KST)
    rows = fetch_day(cfg, fac["fact_code"], stack)
    print(f"[{fac['name']} / factCode {fac['fact_code']} / 배출구 {stack}] 24시간 호출:",
          "실패" if rows is None else f"{len(rows)}건")
    for r in (rows or [])[-6:]:
        print(" ", r.get("open_dt"), "->", parse_open_dt(r.get("open_dt"), now),
              "nox=", r.get("nox_mesure_value"), "co=", r.get("co_mesure_value"), "판정", judge_slot(cfg, r))
    d = fetch_detail(cfg, fac["fact_code"])
    print("\n[상세 호출]", json.dumps(d, ensure_ascii=False, indent=1) if d else "실패")


def cmd_collect(cfg, args):
    con = db_connect(cfg)
    now = datetime.now(KST)
    shift = timedelta(minutes=30) if cfg.get("label_is_end") else timedelta(0)
    ok = fail = new_rows = 0
    net_fail = 0  # 연속 접속 실패 횟수 (3회 연속이면 중단: 서버 접속 불가로 판단)
    aborted = False

    def guarded(fn, *a):
        """접속 예외를 세어 3회 연속이면 전체 중단. return (결과, 예외여부)"""
        nonlocal net_fail
        try:
            res = fn(*a)
            net_fail = 0
            return res, False
        except Exception as e:
            net_fail += 1
            print(f"[ERR] {a}: {e}", file=sys.stderr)
            return None, True

    for fac in cfg["facilities"]:
        label = fac.get("display") or fac["name"]
        fc = str(fac.get("fact_code", "")).strip()
        if not fc:
            print(f"[주의] '{fac['name']}': config 에 fact_code 없음", file=sys.stderr)
            fail += 1
            continue
        detail, _ = guarded(fetch_detail, cfg, fc)
        time.sleep(REQUEST_GAP_SEC)
        if net_fail >= 3:
            aborted = True
            break
        dres = (detail or {}).get("result", {})
        fname = str(dres.get("fact_manage_nm") or fac.get("fact_name") or fac["name"]).strip()
        dtext = dres.get("nox_mesure_value")
        dslot = parse_detail_dt(dres.get("mesure_dt"))
        dslot = floor30(dslot) if dslot else None
        dstack = norm_stack(dres.get("stack_code", "")) if dres.get("stack_code") not in (None, "") else None
        want = [norm_stack(s) for s in stacks_to_collect(fac)] or \
               [norm_stack(s.get("stack_code")) for s in (detail or {}).get("stackList", [])]
        got = 0
        for stack in want:
            rows, _ = guarded(fetch_day, cfg, fc, stack)
            time.sleep(REQUEST_GAP_SEC)
            if net_fail >= 3:
                aborted = True
                break
            if not rows:
                print(f"[주의] '{label}' 배출구 {stack}: 데이터 없음/형식 오류", file=sys.stderr)
                continue
            got += 1
            for rec in rows:
                mdt = parse_open_dt(rec.get("open_dt"), now)
                if mdt is None:
                    continue
                rec = dict(rec)
                # 대표 배출구의 최신 슬롯: 값이 비어 있고 상세 호출에 상태 문구가 있으면 문구를 반영
                if (dstack == stack and dslot is not None and floor30(mdt) == dslot
                        and dtext not in (None, "") and to_float(dtext) is None
                        and all(rec.get(f) in (None, "") for f in MEASURE_FIELDS)):
                    rec["nox_mesure_value"] = str(dtext).strip()
                slot = floor30(mdt) - shift
                running, basis = judge_slot(cfg, rec)
                key = (fname, stack, slot.isoformat())
                old = con.execute("SELECT running, basis FROM readings WHERE facility=? AND stack=? AND slot_ts=?",
                                  key).fetchone()
                raw_s = json.dumps(rec, ensure_ascii=False, separators=(",", ":"))
                if old is None:  # 새 슬롯만 추가 (같은 값을 다시 받으면 DB 를 건드리지 않아 불필요한 커밋 방지)
                    con.execute("INSERT INTO readings VALUES(?,?,?,?,?,?,?,?)",
                                (*key, mdt.isoformat(), running, basis, raw_s, now.isoformat()))
                    new_rows += 1
                elif old != (running, basis) and not is_null_basis(basis):
                    # 같은 슬롯의 값이 구체적으로 정정된 경우에만 갱신 (null 은 기존 기록을 덮어쓰지 않음)
                    con.execute("UPDATE readings SET measured_at=?, running=?, basis=?, raw=?, fetched_at=? "
                                "WHERE facility=? AND stack=? AND slot_ts=?",
                                (mdt.isoformat(), running, basis, raw_s, now.isoformat(), *key))
                    new_rows += 1
        if aborted:
            break
        if got:
            ok += 1
            if got < len(want):
                fail += 1
        else:
            fail += 1
    con.commit()
    if aborted:
        print("[중단] 연속 3회 접속 실패 - 이번 주기 수집을 중단합니다", file=sys.stderr)
    print(f"{now:%Y-%m-%d %H:%M} 수집 성공 {ok}개 사업장 / 실패·일부누락 {fail}개 / 신규·정정 {new_rows}건")
    if ok == 0 and fail > 0:
        sys.exit(1)  # 전부 실패하면 Actions 에서 빨간 표시
    if aborted:
        sys.exit(1)


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


def cmd_slots(cfg, args):
    """완료된 날(오늘 제외)별로 단위(호기)의 30분 슬롯 가동 여부를 JSON 으로 저장 (화면의 발전시간 그래프용).
    파일: <dir>/slots_YYYY-MM-DD.json  값: 48칸 배열(0시, 0시30분 ... 23시30분), 1=발전 0=정지 null=판정불가·미수신"""
    con = db_connect(cfg)
    outdir = args.dir or "reports"
    os.makedirs(outdir, exist_ok=True)
    today = datetime.now(KST).strftime("%Y-%m-%d")
    if args.date:
        days = [args.date]
    else:
        days = [r[0] for r in con.execute("SELECT DISTINCT substr(slot_ts,1,10) FROM readings ORDER BY 1")]
    n_files = 0
    for day in days:
        if day >= today and not args.date:
            continue
        units = build_unit_status(con, cfg, day)
        if not units:
            continue
        interval = detect_interval(cfg, units)
        data = {}
        for dbf, unit, stacks, status in units:
            arr = [None] * 48
            for ts, v in status.items():
                t = datetime.fromisoformat(ts)
                i = t.hour * 2 + (1 if t.minute >= 30 else 0)
                arr[i] = v
                if interval >= 60 and i + 1 < 48:
                    arr[i + 1] = v  # 1시간 간격 자료면 뒤 30분도 같은 값으로 채움
            data[f"{dbf}|{unit}"] = arr
        path = os.path.join(outdir, f"slots_{day}.json")
        text = json.dumps({"day": day, "interval": interval, "units": data}, ensure_ascii=False, separators=(",", ":"))
        if os.path.exists(path) and open(path, encoding="utf-8").read() == text:
            continue  # 내용이 같으면 건드리지 않음(불필요한 커밋 방지)
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(text)
        n_files += 1
    print(f"슬롯 파일 {n_files}개 저장/갱신 ({outdir})")


def cmd_latest(cfg, args):
    """단위(호기)별 가장 최근 슬롯의 발전상태를 JSON 으로 저장 (화면 표시용)."""
    con = db_connect(cfg)
    items = []
    today = datetime.now(KST).strftime("%Y-%m-%d")
    today_units = build_unit_status(con, cfg, today)
    per = detect_interval(cfg, today_units) / 60 if today_units else 0.5
    tmap = {(d, u): st for d, u, _, st in today_units}
    for dbf, unit, stacks, status in build_unit_status(con, cfg):
        ts_ = tmap.get((dbf, unit), {})
        vals = list(ts_.values())
        extra = {  # 금일(0시~최근 게시 슬롯) 누계: 발전 슬롯 수 x 슬롯 길이. 판정불가·누락 슬롯이 있으면 today_partial=true
            "today_hours": vals.count(1) * per,
            "today_on_slots": vals.count(1),
            "today_slots": len(vals),
            "today_upto": max(ts_) if ts_ else None,
            "today_partial": vals.count(None) > 0,
        }
        arr = [None] * 48  # 금일 0시~ 30분 단위 48칸 (1=발전 0=정지 null=판정불가·미게시) — 화면 팝업 그래프용
        for t_, v_ in ts_.items():
            tt = datetime.fromisoformat(t_)
            i_ = tt.hour * 2 + (1 if tt.minute >= 30 else 0)
            arr[i_] = v_
            if per >= 1 and i_ + 1 < 48:
                arr[i_ + 1] = v_
        extra["today_arr"] = arr
        if not status:
            items.append({"facility": dbf, "unit": unit, "stacks": "+".join(stacks), "slot": None, "running": None, **extra})
            continue
        slot = max(status)
        items.append({"facility": dbf, "unit": unit, "stacks": "+".join(stacks), "slot": slot, "running": status[slot], **extra})
    slots = [i["slot"] for i in items if i["slot"]]
    out = {"generated_at": datetime.now(KST).strftime("%Y-%m-%d %H:%M"),
           "latest_slot": max(slots) if slots else None, "items": items}
    for i in items:
        print(f"{i['facility']:<24}{i['unit']:<10}{str(i['slot']):<28}{i['running']}")
    if args.json:
        with open(args.json, "w", encoding="utf-8") as fh:
            json.dump(out, fh, ensure_ascii=False, indent=1)
        print("JSON 저장:", args.json)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["check", "probe", "collect", "status", "report", "latest", "slots"])
    ap.add_argument("--config", default=os.path.join(HERE, "config.json"))
    ap.add_argument("--facility")
    ap.add_argument("--stack")
    ap.add_argument("--date")
    ap.add_argument("--csv")
    ap.add_argument("--json")
    ap.add_argument("--dir")
    args = ap.parse_args()
    cfg = load_config(args.config)
    {"check": cmd_check, "probe": cmd_probe, "collect": cmd_collect, "status": cmd_status,
     "report": cmd_report, "latest": cmd_latest, "slots": cmd_slots}[args.cmd](cfg, args)


if __name__ == "__main__":
    main()
