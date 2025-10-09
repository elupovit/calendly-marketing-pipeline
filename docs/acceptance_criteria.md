
# Acceptance Criteria (Milestone 1)

1) Contracts locked: JSON Schemas validate the provided sample fixtures.
2) Deterministic idempotency: re-posting the same webhook body would not duplicate `invitee_uri + event` rows.
3) Reschedule semantics: one active meeting in Gold for reschedules; historical canceled record remains linked.
4) Host attribution: achievable via Scheduled Event `event_memberships` (Silver).
5) Spend normalization: vendor payloads normalize to one contract; Bronze vs Silver totals within ±2%.
6) Observability: QC metrics + DLQ paths defined.
