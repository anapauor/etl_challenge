-- Layer counts (first-run contract)
SELECT 'bronze.departments_raw' AS t, COUNT(*) AS n FROM bronze.departments_raw
UNION ALL SELECT 'bronze.jobs_raw', COUNT(*) FROM bronze.jobs_raw
UNION ALL SELECT 'bronze.hired_employees_raw', COUNT(*) FROM bronze.hired_employees_raw
UNION ALL SELECT 'silver.departments', COUNT(*) FROM silver.departments
UNION ALL SELECT 'silver.jobs', COUNT(*) FROM silver.jobs
UNION ALL SELECT 'silver.hired_employees', COUNT(*) FROM silver.hired_employees
UNION ALL SELECT 'silver.rejected_records', COUNT(*) FROM silver.rejected_records
UNION ALL SELECT 'gold.m1', COUNT(*) FROM gold.hires_by_department_job_quarter_2021
UNION ALL SELECT 'gold.m2', COUNT(*) FROM gold.departments_above_average_hiring_2021;

-- Duplicate check: must return 0 rows
SELECT id, COUNT(*) AS c
FROM silver.hired_employees
GROUP BY id
HAVING COUNT(*) > 1;
