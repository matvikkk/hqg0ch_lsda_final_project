import os
import json
import csv
import io
import time
import uuid
import datetime as dt
from urllib.request import Request, urlopen
from urllib.parse import urlencode, quote
import boto3

s3 = boto3.client("s3")

def _get_env(name, default=None):
    v = os.environ.get(name, default)
    if v is None or str(v).strip() == "":
        raise KeyError(name)
    return v

def _parse_s3_key(raw_key):
    k = raw_key.strip()
    if k.startswith("s3://"):
        k = k.split("s3://", 1)[1]
        parts = k.split("/", 1)
        return parts[1] if len(parts) == 2 else ""
    return k.lstrip("/")

def _http_get_json(url, timeout=6, headers=None):
    h = {"accept": "application/json"}
    if headers:
        h.update(headers)
    req = Request(url, headers=h, method="GET")
    with urlopen(req, timeout=timeout) as r:
        data = r.read()
    return json.loads(data.decode("utf-8"))

def _load_holidays(year):
    url = f"https://date.nager.at/api/v3/publicholidays/{year}/US"
    data = _http_get_json(url, timeout=10)
    m = {}
    for x in data:
        d = x.get("date")
        name = x.get("name") or x.get("localName") or ""
        if d:
            m[d] = name
    return m

def _timezone_for(lat, lon, tz_cache):
    key = f"{lat:.4f},{lon:.4f}"
    if key in tz_cache:
        return tz_cache[key]
    qs = urlencode({"latitude": str(lat), "longitude": str(lon)})
    url = f"https://timeapi.io/api/TimeZone/coordinate?{qs}"
    try:
        data = _http_get_json(url, timeout=6)
        tz = data.get("timeZone") or ""
    except Exception:
        tz = ""
    tz_cache[key] = tz
    return tz

def _try_parse_dt(s):
    if s is None:
        return None
    t = str(s).strip()
    if t == "":
        return None
    fmts = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%d",
    ]
    for f in fmts:
        try:
            return dt.datetime.strptime(t, f)
        except Exception:
            pass
    return None

def _local_hour_is_night(utc_dt, tz_name):
    if utc_dt is None:
        return (None, None)
    if not tz_name:
        return (utc_dt.hour, 1 if (utc_dt.hour < 6) else 0)
    try:
        from zoneinfo import ZoneInfo
        z = ZoneInfo(tz_name)
        aware = utc_dt.replace(tzinfo=dt.timezone.utc).astimezone(z)
        h = int(aware.hour)
        return (h, 1 if h < 6 else 0)
    except Exception:
        h = int(utc_dt.hour)
        return (h, 1 if h < 6 else 0)

def _sanctions_search(name, sanc_cache):
    q = (name or "").strip()
    if q == "":
        return (0, "", "", 0)
    key = q.lower()
    if key in sanc_cache:
        return sanc_cache[key]
    url = "https://api.sanctions.network/rpc/search_sanctions?name=" + quote(q)
    hit = 0
    sources = ""
    top = ""
    cnt = 0
    try:
        data = _http_get_json(url, timeout=6)
        if isinstance(data, list):
            cnt = len(data)
            if cnt > 0:
                hit = 1
                srcs = []
                for r in data[:5]:
                    s = r.get("source")
                    if s:
                        srcs.append(s)
                sources = ",".join(sorted(set(srcs)))
                top = (data[0].get("names") or [""])[0] if isinstance(data[0].get("names"), list) else (data[0].get("names") or "")
    except Exception:
        pass
    res = (hit, sources, str(top)[:120], cnt)
    sanc_cache[key] = res
    return res

def lambda_handler(event, context):
    start = time.time()
    bucket = _get_env("BUCKET")
    raw_key = _parse_s3_key(_get_env("RAW_KEY"))
    out_prefix = os.environ.get("OUT_PREFIX", "enriched/smoke").strip().strip("/")
    max_rows = int(event.get("max_rows", 25))
    sanc_limit = int(event.get("sanctions_calls_limit", 10))
    do_sanctions = bool(event.get("do_sanctions", True))
    s3_timeout_guard = float(event.get("time_guard_seconds", 45))

    obj = s3.get_object(Bucket=bucket, Key=raw_key)
    body = obj["Body"]

    holidays_cache = {}
    tz_cache = {}
    sanc_cache = {}

    out_key = f"{out_prefix}/smoke_{dt.datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}.csv"

    text_stream = io.TextIOWrapper(body, encoding="utf-8", newline="")
    reader = csv.DictReader(text_stream)
    fieldnames = list(reader.fieldnames or [])

    add_cols = [
        "timezone",
        "local_hour",
        "is_night_transaction",
        "holiday_name",
        "sanctions_hit_merchant",
        "sanctions_sources_merchant",
        "sanctions_hit_person",
        "sanctions_sources_person",
    ]
    for c in add_cols:
        if c not in fieldnames:
            fieldnames.append(c)

    out_buf = io.StringIO()
    writer = csv.DictWriter(out_buf, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()

    processed = 0
    sanc_calls = 0

    for row in reader:
        if processed >= max_rows:
            break
        if (time.time() - start) > s3_timeout_guard:
            break

        lat = row.get("lat")
        lon = row.get("long") or row.get("lon") or row.get("lng") or row.get("longitude")
        try:
            lat_f = float(lat) if lat is not None and str(lat).strip() != "" else None
            lon_f = float(lon) if lon is not None and str(lon).strip() != "" else None
        except Exception:
            lat_f, lon_f = None, None

        tz = ""
        if lat_f is not None and lon_f is not None:
            tz = _timezone_for(lat_f, lon_f, tz_cache)

        tval = row.get("trans_date_trans_time") or row.get("transaction_time") or row.get("Transaction Time") or row.get("trans_time")
        dt_utc = _try_parse_dt(tval)
        local_hour, is_night = _local_hour_is_night(dt_utc, tz)

        holiday_name = ""
        if dt_utc is not None:
            year = dt_utc.year
            if year not in holidays_cache:
                try:
                    holidays_cache[year] = _load_holidays(year)
                except Exception:
                    holidays_cache[year] = {}
            holiday_name = holidays_cache[year].get(dt_utc.strftime("%Y-%m-%d"), "")

        merch = (row.get("merchant") or "").strip()
        first = (row.get("first") or "").strip()
        last = (row.get("last") or "").strip()
        person = (first + " " + last).strip()

        sh_m, ss_m = 0, ""
        sh_p, ss_p = 0, ""

        if do_sanctions and sanc_calls < sanc_limit and merch:
            hit, sources, _, _ = _sanctions_search(merch, sanc_cache)
            sanc_calls += 1
            sh_m, ss_m = hit, sources

        if do_sanctions and sanc_calls < sanc_limit and person and len(person) >= 5:
            hit, sources, _, _ = _sanctions_search(person, sanc_cache)
            sanc_calls += 1
            sh_p, ss_p = hit, sources

        row["timezone"] = tz
        row["local_hour"] = "" if local_hour is None else str(local_hour)
        row["is_night_transaction"] = "" if is_night is None else str(is_night)
        row["holiday_name"] = holiday_name
        row["sanctions_hit_merchant"] = str(sh_m)
        row["sanctions_sources_merchant"] = ss_m
        row["sanctions_hit_person"] = str(sh_p)
        row["sanctions_sources_person"] = ss_p

        writer.writerow(row)
        processed += 1

    data_out = out_buf.getvalue().encode("utf-8")
    s3.put_object(Bucket=bucket, Key=out_key, Body=data_out, ContentType="text/csv")

    return {
        "status": "ok",
        "bucket": bucket,
        "raw_key": raw_key,
        "out_key": out_key,
        "processed_rows": processed,
        "sanctions_calls_used": sanc_calls,
        "timezone_cache_size": len(tz_cache),
        "elapsed_seconds": round(time.time() - start, 3)
    }
