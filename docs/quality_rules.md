
# Quality Rules

## Calendly (Bronze → Silver)
1. Schema conformance: park failures in DLQ.
2. Uniqueness: `payload.uri` unique per `event`; duplicates are no-ops.
3. Timestamp validity: `payload.created_at`, `payload.updated_at` must be ISO-8601; skew ≤ 7 days.
4. Reschedule linkage: if `rescheduled=true`, expect `old_invitee` or `new_invitee` non-null; pair canceled↔created.
5. Enrichment: `payload.event` must be a valid Scheduled Event URI; retry on transient failures; mark `enrichment_status` on failure.

## Spend (Bronze → Silver)
1. Required: `source`, `date`, `metrics.spend`, `currency`.
2. Types: numeric metrics ≥ 0; allow null → coalesce in Silver.
3. Currency: ISO-4217 3-letter; else DLQ.
4. Deduping: `(source,date,campaign_id,currency,granularity,load_id)`; pick latest by `extracted_at`.
5. Totals check: per `(source,date,account)` aggregates Bronze vs Silver within ±2%.
