# repair_partitions.py  (Glue Python Shell job, Python 3.12)
import time, boto3, os

ATHENA_DB = "calendly_marketing"
WORKGROUP = os.getenv("ATHENA_WORKGROUP", "primary")  # or your WG

TABLES = [
    "gold_bookings_by_source_day",
    "gold_bookings_trend_over_time",
    "gold_bookings_by_timeslot",
    "gold_cost_per_booking_by_channel",
    "gold_channel_attribution_leaderboard",
    "gold_meetings_per_employee_week",
]

athena = boto3.client("athena")

def run(sql):
    r = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": ATHENA_DB},
        WorkGroup=WORKGROUP,
    )
    qid = r["QueryExecutionId"]
    while True:
        s = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]["State"]
        if s in ("SUCCEEDED","FAILED","CANCELLED"): break
        time.sleep(1.0)
    if s != "SUCCEEDED":
        raise RuntimeError(f"Athena failed for: {sql}")
    return qid

for t in TABLES:
    print(f"Repairing {t}…")
    run(f"MSCK REPAIR TABLE {ATHENA_DB}.{t}")
print("✅ Partitions repaired")
