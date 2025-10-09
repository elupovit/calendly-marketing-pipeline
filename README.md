
# Milestone 1 — Source Recon (Artifacts)

**Region:** us-east-1  
**Bucket:** calendly-marketing-pipeline-data (SSE-S3)  
**Alerts:** eitlup94@gmail.com  
**Signature Verification:** false

## Paths
- `schemas/calendly_webhook_bronze.schema.json`
- `schemas/marketing_spend_bronze.schema.json`
- `tests/fixtures/calendly_invitee_created_sample.json`
- `tests/fixtures/calendly_invitee_canceled_sample.json`
- `tests/fixtures/marketing_spend_samples.json`
- `docs/calendly_bronze_dictionary.csv`
- `docs/marketing_spend_bronze_dictionary.csv`
- `docs/quality_rules.md`
- `docs/validation_checklist.md`
- `docs/acceptance_criteria.md`

## Dedupe Keys
- Calendly: `payload.uri` + `event`
- Marketing Spend: `(source, date, campaign_id, currency, granularity, load_id)`

## Partitioning
- Use `dt=YYYY-MM-DD` derived from `payload.created_at` for Calendly;
- Use `dt=date` for spend.

