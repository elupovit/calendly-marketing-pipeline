import boto3
import awswrangler as wr

# ---------- CONFIG ----------
PROFILE = "calendly-pipeline"
REGION = "us-east-1"
S3_BUCKET = "calendly-marketing-pipeline-data"
GLUE_DB = "calendly_marketing"

session = boto3.Session(profile_name=PROFILE, region_name=REGION)

# ---------- S3: simple sanity (list a few objects) ----------
def test_s3():
    print("\n🧩 Testing S3 connection (list only)...")
    try:
        s3 = session.client("s3")
        res = s3.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix="gold/calendly/",
            MaxKeys=10,
        )
        contents = res.get("Contents", [])
        if not contents:
            print("⚠️  No objects under gold/calendly/")
        else:
            print("✅ S3 reachable — sample keys:")
            for o in contents[:5]:
                print(f"   - {o['Key']}")
    except Exception as e:
        print(f"❌ S3 list failed: {e}")

# ---------- Athena queries with exact schemas ----------
QUERIES = {
    # 1.1
    "gold_bookings_by_source_day": """
        SELECT dt, booking_date, source, booking_id
        FROM calendly_marketing.gold_bookings_by_source_day
        ORDER BY dt DESC
        LIMIT 5
    """,
    # 1.3
    "gold_bookings_trend_over_time": """
        SELECT dt, booking_date, source, booking_id
        FROM calendly_marketing.gold_bookings_trend_over_time
        ORDER BY dt DESC
        LIMIT 5
    """,
    # 1.5
    "gold_bookings_by_timeslot": """
        SELECT dt, event_start_local, hour_of_day, day_of_week, source, booking_id
        FROM calendly_marketing.gold_bookings_by_timeslot
        ORDER BY dt DESC
        LIMIT 5
    """,
    # 1.4
    "gold_channel_attribution_leaderboard": """
        SELECT dt, source, campaign, total_bookings, total_spend, cpb
        FROM calendly_marketing.gold_channel_attribution_leaderboard
        ORDER BY dt DESC
        LIMIT 5
    """,
    # 1.2
    "gold_cost_per_booking_by_channel": """
        SELECT dt, channel, total_bookings, total_spend, cpb
        FROM calendly_marketing.gold_cost_per_booking_by_channel
        ORDER BY dt DESC
        LIMIT 5
    """,
    # 1.6
    "gold_meetings_per_employee_week": """
        SELECT dt, employee_id, meeting_id, meeting_date, week_of_year, year, meetings, avg_meetings_per_week
        FROM calendly_marketing.gold_meetings_per_employee_week
        ORDER BY dt DESC
        LIMIT 5
    """,
}

def test_athena():
    print("\n🧠 Testing Athena/Glue queries...")
    for name, sql in QUERIES.items():
        print(f"\n▶️ {name}")
        try:
            df = wr.athena.read_sql_query(sql, database=GLUE_DB, boto3_session=session)
            print(f"✅ {len(df)} rows")
            if not df.empty:
                print(df.head().to_string(index=False))
        except Exception as e:
            print(f"❌ Query failed: {e}")

if __name__ == "__main__":
    test_s3()
    test_athena()
