# Strict validation

SchemaBridge always runs generated aggregate validation after confirmed execution. It compares row counts plus mapping-aware null and distinct counts. That is fast and useful, but it does not prove that every individual row is present.

Strict primary-key validation is an optional second check. Enable it from the React validation screen or by sending `strict_primary_key: true` to `POST /api/v1/migrations/workflows/{workflow_id}/validate`. The default key-page size is 500 and can be changed with the positive integer `primary_key_batch_size`.

```json
{
  "expected_version": 7,
  "execution_evidence_artifact_version": 12,
  "approved_mapping_artifact_version": 8,
  "source_profile_id": "postgres-source",
  "target_profile_id": "postgres-target",
  "strict_primary_key": true,
  "primary_key_batch_size": 500
}
```

The normal workflow request requirements still apply, including an `Idempotency-Key` header. The React interface uses the default page size and shows counts only.

## What the strict check proves

When eligible, SchemaBridge reads each table's primary-key columns through internally generated, read-only, ordered pages. It compares the full key multisets and records only these counts:

- source and target key counts;
- keys missing from the target;
- keys present only in the target;
- duplicate-key occurrences on either side.

No primary-key values are added to API responses, artifacts, or audit events. A mismatch changes the validation result to review-required and adds the `STRICT_PRIMARY_KEY_MISMATCH` warning.

## Eligibility and fallback

Strict validation is deliberately conservative. It runs only when the durable source and target discovery artifacts show complete primary-key metadata, both tables declare a non-empty primary key, and every source key column is approved as a direct copy to the corresponding target key column in the same order.

Computed expressions, casts, renamed/reordered key mappings, incomplete discovery metadata, and tables without usable keys do not qualify. In that case aggregate validation still runs, but the report is `INCOMPLETE` with one of these safe reason codes:

- `PRIMARY_KEY_METADATA_INCOMPLETE`
- `PRIMARY_KEY_UNAVAILABLE`
- `PRIMARY_KEY_MAPPING_INCOMPLETE`
- `PRIMARY_KEY_MAPPING_MISMATCH`

This makes the fallback explicit: SchemaBridge never pretends that an aggregate-only result was an exact key comparison.

## Scale boundary

The database reads are bounded pages, but the current exact comparison accumulates key counters in the SchemaBridge process. Use this optional mode for moderate-sized tables where that memory cost is acceptable. Very large-table hash reconciliation or an external sort is future work; automatic Spark transport does not change this validation mode.

SchemaBridge also reads the source and target as separate database operations; it does not create a shared cross-database snapshot. Run validation after the migration against tables that are not changing, or otherwise treat its result as evidence for that observed point in time.

## PostgreSQL duplicate-key limitation

PostgreSQL enforces a declared primary key with a unique index. Therefore it will reject an attempt to insert two rows with the same declared primary-key value. A realistic live PostgreSQL proof can safely demonstrate missing and extra keys, but not a duplicate primary key.

Duplicate counting is still covered by credential-free unit tests and remains useful for systems where key metadata can be informational rather than enforced, including suitable Snowflake designs. The opt-in PostgreSQL integration proof is enabled with `SCHEMABRIDGE_STRICT_VALIDATION_POSTGRES_INTEGRATION=1` and uses disposable random tables; it cleans them up after the test.
