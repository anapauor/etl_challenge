-- Hired-employees reconciliation (first-run / same batch_id)
-- Identity: bronze_rows = silver_ids + rejected_he  =>  31 = 20 + 11
-- Of the 11 rejects, exactly 1 is duplicate_id_loser (day-01 id 1008).
--
-- With a NEW batch_id, Bronze appends another 31 rows (delete-by-batch only
-- removes the current batch), so Bronze grows while Silver stays at unique
-- ids via MERGE. Do not expect bronze_rows = silver_ids + rejected_he after
-- a second distinct batch.
SELECT
  (SELECT COUNT(*) FROM bronze.hired_employees_raw) AS bronze_rows,
  (SELECT COUNT(*) FROM silver.hired_employees) AS silver_ids,
  (SELECT COUNT(*) FROM silver.rejected_records WHERE table_name = 'hired_employees') AS rejected_he,
  (
    SELECT COUNT(*)
    FROM silver.rejected_records
    WHERE table_name = 'hired_employees'
      AND rejection_reason LIKE '%duplicate_id_loser%'
  ) AS dup_losers;
