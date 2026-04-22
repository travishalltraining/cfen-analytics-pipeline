import os
import json
import requests
from datetime import datetime, date, timedelta, timezone
from zoneinfo import ZoneInfo

from google.cloud import bigquery


PROJECT_ID = os.environ["GCP_PROJECT_ID"]
WODIFY_API_KEY = os.environ["WODIFY_API_KEY"]

DATASET_ID = "cfen_analytics"
BASE_URL = "https://api.wodify.com/v1"
TIMEZONE = ZoneInfo("America/Chicago")

SNAPSHOT_DATE = datetime.now(TIMEZONE).date()
YESTERDAY = SNAPSHOT_DATE - timedelta(days=1)

BQ = bigquery.Client(project=PROJECT_ID)


def wodify_get(path, params=None):
    if params is None:
        params = {}

    response = requests.get(
        f"{BASE_URL}{path}",
        headers={
            "accept": "application/json",
            "x-api-key": WODIFY_API_KEY,
        },
        params=params,
        timeout=30,
    )

    if not response.ok:
        raise RuntimeError(f"Wodify API error {response.status_code}: {response.text}")

    return response.json()


def normalize_array(payload):
    if isinstance(payload, list):
        return payload

    for key in [
        "clients",
        "client_sign_ins",
        "clientSignIns",
        "sign_ins",
        "signIns",
        "class_sign_ins",
        "classSignIns",
        "data",
        "items",
        "results",
        "records",
    ]:
        value = payload.get(key)
        if isinstance(value, list):
            return value

    return []


def fetch_paged(path, page_size=100, max_pages=10, extra_params=None):
    if extra_params is None:
        extra_params = {}

    all_rows = []

    for page in range(1, max_pages + 1):
        params = {
            "page": page,
            "page_size": page_size,
            **extra_params,
        }

        payload = wodify_get(path, params=params)
        rows = normalize_array(payload)

        all_rows.extend(rows)

        if len(rows) < page_size:
            break

    return all_rows


def safe_int(value):
    try:
        if value is None or value == "":
            return None
        return int(value)
    except Exception:
        return None


def safe_bool(value):
    if isinstance(value, bool):
        return value
    if value is None:
        return None

    text = str(value).lower().strip()

    if text in ["true", "1", "yes"]:
        return True
    if text in ["false", "0", "no"]:
        return False

    return None


def parse_date(value):
    if not value:
        return None

    text = str(value)

    if text.startswith("1900"):
        return None

    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date().isoformat()
    except Exception:
        try:
            return datetime.strptime(text[:10], "%Y-%m-%d").date().isoformat()
        except Exception:
            return None


def parse_timestamp(value):
    if not value:
        return None

    text = str(value)

    if text.startswith("1900"):
        return None

    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).isoformat()
    except Exception:
        return None


def get_local_class_date(sign_in):
    raw = sign_in.get("local_class_start_datetime")
    if not raw:
        return None

    # Wodify sends local class time as a string. Do not timezone convert it.
    return str(raw)[:10]


def get_local_class_time(sign_in):
    raw = sign_in.get("local_class_start_datetime")
    if not raw or "T" not in str(raw):
        return None

    try:
        time_part = str(raw).split("T")[1][:5]
        hour = int(time_part[:2])
        minute = time_part[3:5]

        suffix = "AM" if hour < 12 else "PM"
        display_hour = hour % 12
        if display_hour == 0:
            display_hour = 12

        return f"{display_hour}:{minute} {suffix}"
    except Exception:
        return None


def get_day_of_week(date_string):
    if not date_string:
        return None

    try:
        d = datetime.strptime(date_string, "%Y-%m-%d").date()
        return d.strftime("%A")
    except Exception:
        return None


def clean_display_name(first_name, last_name):
    first = first_name or ""
    last = last_name or ""
    return f"{first} {last}".strip()


def get_days_since_last_attendance(client):
    value = safe_int(client.get("days_since_last_attendance"))

    if value is not None and value > 0:
        return value

    last_attendance = client.get("last_attendance") or client.get("last_class_sign_in")

    if not last_attendance or str(last_attendance).startswith("1900"):
        return None

    parsed = parse_date(last_attendance)
    if not parsed:
        return None

    last_date = datetime.strptime(parsed, "%Y-%m-%d").date()
    return (SNAPSHOT_DATE - last_date).days


def is_active_client(client):
    return str(client.get("client_status", "")).lower() == "active"


def birthday_matches(client, target_date):
    birthday = parse_date(client.get("date_of_birth"))

    if not birthday:
        return False

    bday = datetime.strptime(birthday, "%Y-%m-%d").date()

    return bday.month == target_date.month and bday.day == target_date.day


def birthday_in_next_7_days(client):
    birthday = parse_date(client.get("date_of_birth"))

    if not birthday:
        return False

    bday = datetime.strptime(birthday, "%Y-%m-%d").date()

    for offset in range(0, 8):
        target = SNAPSHOT_DATE + timedelta(days=offset)
        if bday.month == target.month and bday.day == target.day:
            return True

    return False


def delete_for_today():
    BQ.query(
        f"""
        DELETE FROM `{PROJECT_ID}.{DATASET_ID}.members_daily_snapshot`
        WHERE snapshot_date = @snapshot_date
        """,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("snapshot_date", "STRING", SNAPSHOT_DATE.isoformat())
            ]
        ),
    ).result()

    BQ.query(
        f"""
        DELETE FROM `{PROJECT_ID}.{DATASET_ID}.daily_summary`
        WHERE report_date = @report_date
        """,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("report_date", "STRING", SNAPSHOT_DATE.isoformat())
            ]
        ),
    ).result()

    # Re-sync a rolling window of class sign-ins to avoid duplicates.
    window_start = SNAPSHOT_DATE - timedelta(days=30)
    window_end = SNAPSHOT_DATE + timedelta(days=14)

    BQ.query(
        f"""
        DELETE FROM `{PROJECT_ID}.{DATASET_ID}.class_sign_ins`
        WHERE class_date BETWEEN @window_start AND @window_end
        """,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("window_start", "STRING", window_start.isoformat()),
                bigquery.ScalarQueryParameter("window_end", "STRING", window_end.isoformat()),
            ]
        ),
    ).result()


def insert_rows(table_name, rows):
    if not rows:
        print(f"No rows to insert for {table_name}.")
        return

    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    errors = BQ.insert_rows_json(table_ref, rows)

    if errors:
        raise RuntimeError(f"BigQuery insert failed for {table_name}: {errors}")

    print(f"Inserted {len(rows)} rows into {table_name}.")


def build_member_rows(clients):
    synced_at = datetime.now(timezone.utc).isoformat()

    rows = []

    for client in clients:
        first_name = client.get("first_name")
        last_name = client.get("last_name")

        rows.append(
            {
                "snapshot_date": SNAPSHOT_DATE.isoformat(),
                "client_id": safe_int(client.get("id")),
                "first_name": first_name,
                "last_name": last_name,
                "display_name": clean_display_name(first_name, last_name),
                "email": client.get("email"),
                "phone_number": client.get("phone_number"),
                "client_status": client.get("client_status"),
                "location": client.get("location"),
                "default_program": client.get("default_program"),
                "date_of_birth": parse_date(client.get("date_of_birth")),
                "member_since": parse_date(client.get("member_since")),
                "last_attendance": parse_timestamp(client.get("last_attendance")),
                "days_since_last_attendance": get_days_since_last_attendance(client),
                "total_class_sign_ins": safe_int(client.get("total_class_sign_ins")),
                "total_booking_sign_ins": safe_int(client.get("total_booking_sign_ins")),
                "current_weekstreak": safe_int(client.get("current_weekstreak")),
                "highest_weekstreak": safe_int(client.get("highest_weekstreak")),
                "is_at_risk": safe_bool(client.get("is_at_risk")),
                "raw_json": json.dumps(client),
                "synced_at": synced_at,
            }
        )

    return rows


def build_sign_in_rows(sign_ins):
    synced_at = datetime.now(timezone.utc).isoformat()

    rows = []

    window_start = SNAPSHOT_DATE - timedelta(days=30)
    window_end = SNAPSHOT_DATE + timedelta(days=14)

    for sign_in in sign_ins:
        class_date = get_local_class_date(sign_in)

        if not class_date:
            continue

        try:
            parsed_class_date = datetime.strptime(class_date, "%Y-%m-%d").date()
        except Exception:
            continue

        if parsed_class_date < window_start or parsed_class_date > window_end:
            continue

        rows.append(
            {
                "sign_in_id": safe_int(sign_in.get("id")),
                "client_id": safe_int(sign_in.get("client_id")),
                "client_name": sign_in.get("client"),
                "email": sign_in.get("email"),
                "class_id": safe_int(sign_in.get("class_id")),
                "class_name": sign_in.get("class"),
                "program": sign_in.get("program"),
                "membership": sign_in.get("membership"),
                "location": sign_in.get("location"),
                "sign_in_source": sign_in.get("sign_in_source"),
                "sign_in_date_time": parse_timestamp(sign_in.get("sign_in_date_time")),
                "local_class_start_datetime": sign_in.get("local_class_start_datetime"),
                "class_date": class_date,
                "class_time": get_local_class_time(sign_in),
                "class_day_of_week": get_day_of_week(class_date),
                "raw_json": json.dumps(sign_in),
                "synced_at": synced_at,
            }
        )

    return rows


def build_daily_summary(clients, sign_ins):
    active_clients = [c for c in clients if is_active_client(c)]

    risk_7_to_14 = 0
    risk_15_to_29 = 0
    risk_30_plus = 0
    new_member_watchlist = 0
    birthdays_today = 0
    birthdays_this_week = 0

    for client in active_clients:
        days_since = get_days_since_last_attendance(client)

        if days_since is not None:
            if 7 <= days_since <= 14:
                risk_7_to_14 += 1
            elif 15 <= days_since <= 29:
                risk_15_to_29 += 1
            elif days_since >= 30:
                risk_30_plus += 1

        member_since = parse_date(client.get("member_since"))
        if member_since:
            joined = datetime.strptime(member_since, "%Y-%m-%d").date()
            days_since_join = (SNAPSHOT_DATE - joined).days
            total_visits = safe_int(client.get("total_class_sign_ins")) or 0

            if 0 <= days_since_join <= 30:
                if total_visits < 4 or days_since is None or days_since >= 7:
                    new_member_watchlist += 1

        if birthday_matches(client, SNAPSHOT_DATE):
            birthdays_today += 1

        if birthday_in_next_7_days(client):
            birthdays_this_week += 1

    total_sign_ins_yesterday = 0

    for sign_in in sign_ins:
        class_date = get_local_class_date(sign_in)
        if class_date == YESTERDAY.isoformat():
            total_sign_ins_yesterday += 1

    return [
        {
            "report_date": SNAPSHOT_DATE.isoformat(),
            "active_memberships": len(active_clients),
            "total_class_sign_ins_yesterday": total_sign_ins_yesterday,
            "risk_7_to_14_count": risk_7_to_14,
            "risk_15_to_29_count": risk_15_to_29,
            "risk_30_plus_count": risk_30_plus,
            "new_member_watchlist_count": new_member_watchlist,
            "birthdays_today_count": birthdays_today,
            "birthdays_this_week_count": birthdays_this_week,
            "synced_at": datetime.now(timezone.utc).isoformat(),
        }
    ]


def main():
    print("Pulling Wodify clients...")
    clients = fetch_paged("/clients", page_size=100, max_pages=10)

    print(f"Pulled {len(clients)} clients.")

    print("Pulling Wodify class sign-ins...")
    sign_ins = fetch_paged(
        "/classes/sign-ins/clients",
        page_size=1000,
        max_pages=5,
        extra_params={"sort": "desc_id"},
    )

    print(f"Pulled {len(sign_ins)} sign-ins.")

    print("Deleting today/rolling-window rows...")
    delete_for_today()

    print("Building rows...")
    member_rows = build_member_rows(clients)
    sign_in_rows = build_sign_in_rows(sign_ins)
    daily_summary_rows = build_daily_summary(clients, sign_ins)

    print("Writing to BigQuery...")
    insert_rows("members_daily_snapshot", member_rows)
    insert_rows("class_sign_ins", sign_in_rows)
    insert_rows("daily_summary", daily_summary_rows)

    print("Wodify sync complete.")


if __name__ == "__main__":
    main()
