import requests
import json
from db import get_db_connection

API_KEY = "579b464db66ec23bdd000001cdd3946e44ce4aad7209ff7b23ac571b"
RESOURCE_ID = "ee03643a-ee4c-48c2-ac30-9f2ff26ab722"
API_URL = f"https://api.data.gov.in/resource/ee03643a-ee4c-48c2-ac30-9f2ff26ab722?api-key=579b464db66ec23bdd000001cdd3946e44ce4aad7209ff7b23ac571b&format=json"

def fetch_api_data():
    response = requests.get(API_URL)
    if response.status_code == 200:
        data = response.json()
        records = data.get('records', [])
        print(f"✅ Fetched {len(records)} records from API")
        return records
    else:
        print("❌ Failed to fetch API data", response.status_code)
        return []

def save_to_db(records):
    conn = get_db_connection()
    if conn is None:
        return
    cur = conn.cursor()

    month_map = {"Jan":1,"Feb":2,"Mar":3,"Apr":4,"May":5,"Jun":6,
                 "Jul":7,"Aug":8,"Sep":9,"Oct":10,"Nov":11,"Dec":12}

    for r in records:
        state_name = r.get('state_name')
        district_name = r.get('district_name')
        fin_year = r.get('fin_year', "2025-2026")
        month_name = r.get('month')
        month = month_map.get(month_name[:3], 0)
        year = int(fin_year.split('-')[0])

        jobcards = int(r.get('Total_No_of_JobCards_issued', r.get('total_no_of_jobcards', 0)))
        workers = int(r.get('Total_No_of_Workers', r.get('total_workers', 0)))

        # Insert State
        cur.execute("""
            INSERT INTO states(name, code)
            VALUES(%s, %s)
            ON CONFLICT (code) DO NOTHING
            RETURNING id
        """, (state_name, state_name[:3].upper()))
        state_id = cur.fetchone()
        if state_id is None:
            cur.execute("SELECT id FROM states WHERE code=%s", (state_name[:3].upper(),))
            state_id = cur.fetchone()
        state_id = state_id[0]

        # Insert District
        cur.execute("""
            INSERT INTO districts(name, state_id, code)
            VALUES(%s, %s, %s)
            ON CONFLICT (name) DO NOTHING
            RETURNING id
        """, (district_name, state_id, district_name[:3].upper()))
        district_id = cur.fetchone()
        if district_id is None:
            cur.execute("SELECT id FROM districts WHERE name=%s", (district_name,))
            district_id = cur.fetchone()
        district_id = district_id[0]

        # Jobcards Metric
        metric_key = 'jobcards_issued'
        cur.execute("""
            INSERT INTO metric_types(key, label_en)
            VALUES(%s, %s)
            ON CONFLICT (key) DO NOTHING
            RETURNING id
        """, (metric_key, 'Total Jobcards Issued'))
        metric_id = cur.fetchone()
        if metric_id is None:
            cur.execute("SELECT id FROM metric_types WHERE key=%s", (metric_key,))
            metric_id = cur.fetchone()
        metric_id = metric_id[0]

        cur.execute("""
            INSERT INTO district_monthly_metrics(
                district_id, metric_type_id, year, month, metric_value, source_json
            ) VALUES(%s, %s, %s, %s, %s, %s)
            ON CONFLICT (district_id, metric_type_id, year, month) DO UPDATE
            SET metric_value=EXCLUDED.metric_value, source_json=EXCLUDED.source_json
        """, (district_id, metric_id, year, month, jobcards, json.dumps(r)))

        # Workers Metric
        metric_key = 'total_workers'
        cur.execute("""
            INSERT INTO metric_types(key, label_en)
            VALUES(%s, %s)
            ON CONFLICT (key) DO NOTHING
            RETURNING id
        """, (metric_key, 'Total Workers'))
        metric_id = cur.fetchone()
        if metric_id is None:
            cur.execute("SELECT id FROM metric_types WHERE key=%s", (metric_key,))
            metric_id = cur.fetchone()
        metric_id = metric_id[0]

        cur.execute("""
            INSERT INTO district_monthly_metrics(
                district_id, metric_type_id, year, month, metric_value, source_json
            ) VALUES(%s, %s, %s, %s, %s, %s)
            ON CONFLICT (district_id, metric_type_id, year, month) DO UPDATE
            SET metric_value=EXCLUDED.metric_value, source_json=EXCLUDED.source_json
        """, (district_id, metric_id, year, month, workers, json.dumps(r)))

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Data inserted successfully")

if __name__ == "__main__":
    records = fetch_api_data()
    if records:
        save_to_db(records)
