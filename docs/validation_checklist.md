
# Validation Checklist (Milestone 1)

- [x] Environment confirmed (us-east-1, bucket `calendly-marketing-pipeline-data`, SSE-S3, alerts email).
- [x] Webhook events chosen: invitee.created + invitee.canceled (routing form optional).
- [x] Bronze schemas saved (Calendly + Spend).
- [x] Dedupe keys defined (Calendly: `payload.uri`+`event`; Spend: composite).
- [x] Reschedule handling documented and sample pair created.
- [x] Silver enrichment inputs defined (Scheduled Event → event_memberships for host).
- [x] QC rules documented.
- [x] Partitioning: `dt` from source timestamps (UTC).
- [x] Provenance fields captured in spend.
- [x] Sample fixtures created and ready to validate.
