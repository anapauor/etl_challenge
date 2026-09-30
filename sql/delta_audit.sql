-- Delta Lake audit for silver.hired_employees
-- In this warehouse, VERSION AS OF 0 already has the first 20 rows
-- (CREATE OR REPLACE / overwrite from the initial Silver load). Later
-- versions are MERGE reruns of the same batch.

DESCRIBE HISTORY silver.hired_employees;

SELECT COUNT(*) AS n_version_0
FROM silver.hired_employees VERSION AS OF 0;

-- Optional Change Data Feed (disabled by default on local Delta):
-- ALTER TABLE silver.hired_employees SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
-- SELECT * FROM table_changes('silver.hired_employees', 0);
