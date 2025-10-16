#!/usr/bin/env python3
"""
Deploy Glue scripts to S3 and auto-update Glue job ScriptLocation.

- Syncs .py and .jar files under /scripts → s3://<bucket>/scripts/
- Writes a _manifest.json with SHA256 checksums
- Updates Glue job ScriptLocation safely (skipping None fields)
- Idempotent and safe to re-run
"""

import os, sys, json, hashlib, time, pathlib
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError

# --- Configuration ---
BUCKET = os.getenv("GLUE_SCRIPTS_BUCKET", "calendly-marketing-pipeline-data")
PREFIX = os.getenv("GLUE_SCRIPTS_PREFIX", "scripts/")
DEFAULT_JOBS = [
    "bronze_to_silver_calendly",
    "calendly-silver-to-gold",
    "optimize-gold-delta",
    "validate-gold-quality",
]
JOB_NAMES = [j.strip() for j in os.getenv("GLUE_JOB_NAMES", ",".join(DEFAULT_JOBS)).split(",") if j.strip()]
LOCAL_DIR = os.getenv("LOCAL_SCRIPTS_DIR", "scripts")
MANIFEST_KEY = f"{PREFIX}_manifest.json"

s3 = boto3.client("s3")
glue = boto3.client("glue")

# --- Utilities ---
def sha256_file(p: pathlib.Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def load_manifest():
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=MANIFEST_KEY)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return {"files": {}, "updated_at": None}
        raise

def save_manifest(m):
    m["updated_at"] = datetime.now(timezone.utc).isoformat()
    s3.put_object(Bucket=BUCKET, Key=MANIFEST_KEY, Body=json.dumps(m, indent=2).encode("utf-8"))

def upload_if_changed(local_path: pathlib.Path, manifest, dry_run=False):
    rel = local_path.relative_to(LOCAL_DIR).as_posix()
    key = f"{PREFIX}{rel}"
    digest = sha256_file(local_path)
    entry = manifest["files"].get(rel)
    if entry and entry.get("sha256") == digest:
        return {"uploaded": False, "key": key, "rel": rel, "digest": digest}
    if not dry_run:
        s3.upload_file(str(local_path), BUCKET, key)
    manifest["files"][rel] = {"sha256": digest, "s3_key": key, "ts": int(time.time())}
    return {"uploaded": True, "key": key, "rel": rel, "digest": digest}

# --- Glue helpers ---
def current_script_location(job_name):
    job = glue.get_job(JobName=job_name)["Job"]
    return job["Command"].get("ScriptLocation")

def update_job_script(job_name, new_s3_uri, dry_run=False):
    """Safely update a Glue job ScriptLocation, ignoring None fields and removing invalid combinations."""
    job = glue.get_job(JobName=job_name)["Job"]
    cmd = job["Command"]

    if cmd.get("ScriptLocation") == new_s3_uri:
        return False

    cmd["ScriptLocation"] = new_s3_uri

    # Build JobUpdate dict safely
    job_update = {"Role": job["Role"], "Command": cmd}

    optional_keys = [
        "ExecutionProperty", "DefaultArguments", "GlueVersion",
        "NumberOfWorkers", "WorkerType", "MaxRetries", "Timeout",
        "SecurityConfiguration", "NotificationProperty", "Connections",
        "Tags", "MaxCapacity"
    ]
    for k in optional_keys:
        v = job.get(k)
        if v is not None:
            job_update[k] = v

    # --- AWS quirk fix ---
    # Cannot have both MaxCapacity and (WorkerType/NumberOfWorkers)
    if "WorkerType" in job_update or "NumberOfWorkers" in job_update:
        job_update.pop("MaxCapacity", None)

    if not dry_run:
        glue.update_job(JobName=job_name, JobUpdate=job_update)
    return True

# --- Main logic ---
def main():
    import argparse
    ap = argparse.ArgumentParser(description="Sync Glue scripts to S3 and update Glue job ScriptLocation.")
    ap.add_argument("--dry-run", action="store_true", help="Do not upload or update jobs")
    args = ap.parse_args()

    if not os.path.isdir(LOCAL_DIR):
        print(f"ERROR: Local scripts dir not found: {LOCAL_DIR}", file=sys.stderr)
        sys.exit(2)

    manifest = load_manifest()
    changed = []

    for p in pathlib.Path(LOCAL_DIR).rglob("*"):
        if p.is_dir() or p.suffix.lower() not in {".py", ".jar"}:
            continue
        res = upload_if_changed(p, manifest, dry_run=args.dry_run)
        if res["uploaded"]:
            changed.append(res)

    if changed:
        print("Uploaded/changed files:")
        for c in changed:
            print(f"  s3://{BUCKET}/{c['key']}  ({c['rel']})")
        if not args.dry_run:
            save_manifest(manifest)
            print(f"Manifest updated: s3://{BUCKET}/{MANIFEST_KEY}")
    else:
        print("No script changes detected.")

    mapping_env = os.getenv("GLUE_JOB_MAP", "")
    mapping = {}
    if mapping_env.strip():
        for pair in mapping_env.split(","):
            j, s = pair.split(":")
            mapping[j.strip()] = s.strip()
    else:
        for j in JOB_NAMES:
            mapping[j] = f"{j}.py"

    print("\nGlue job ScriptLocation checks:")
    for job in JOB_NAMES:
        script_rel = mapping.get(job)
        if not script_rel:
            print(f"- {job}: SKIP (no mapping)")
            continue
        s3_key = f"{PREFIX}{script_rel}"
        s3_uri = f"s3://{BUCKET}/{s3_key}"
        try:
            current = current_script_location(job)
            needs = (current != s3_uri)
            changed_flag = update_job_script(job, s3_uri, dry_run=args.dry_run) if needs else False
            status = "updated" if changed_flag else ("ok" if not needs else "pending(dry-run)")
            print(f"- {job}: {status} -> {s3_uri}")
        except ClientError as e:
            print(f"- {job}: ERROR {e}")
    print("\nDone.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
# trigger workflow
