-- Gold metric 1: hires by department and job, pivoted by quarter (2021)
-- DROP + CREATE USING DELTA keeps this runnable on local Delta and Databricks.
DROP TABLE IF EXISTS gold.hires_by_department_job_quarter_2021;

CREATE TABLE gold.hires_by_department_job_quarter_2021
USING DELTA
AS
SELECT
  d.department,
  j.job,
  SUM(CASE WHEN quarter(e.hired_at) = 1 THEN 1 ELSE 0 END) AS Q1,
  SUM(CASE WHEN quarter(e.hired_at) = 2 THEN 1 ELSE 0 END) AS Q2,
  SUM(CASE WHEN quarter(e.hired_at) = 3 THEN 1 ELSE 0 END) AS Q3,
  SUM(CASE WHEN quarter(e.hired_at) = 4 THEN 1 ELSE 0 END) AS Q4
FROM silver.hired_employees e
JOIN silver.departments d ON e.department_id = d.id
JOIN silver.jobs j ON e.job_id = j.id
WHERE year(e.hired_at) = 2021
GROUP BY d.department, j.job
ORDER BY d.department ASC, j.job ASC;

-- Gold metric 2: departments that hired above the 2021 per-department average
DROP TABLE IF EXISTS gold.departments_above_average_hiring_2021;

CREATE TABLE gold.departments_above_average_hiring_2021
USING DELTA
AS
WITH hires AS (
  SELECT d.id, d.department, COUNT(*) AS hired
  FROM silver.hired_employees e
  JOIN silver.departments d ON e.department_id = d.id
  WHERE year(e.hired_at) = 2021
  GROUP BY d.id, d.department
),
avg_h AS (
  SELECT AVG(hired) AS avg_hired FROM hires
)
SELECT h.id, h.department, h.hired
FROM hires h
CROSS JOIN avg_h a
WHERE h.hired > a.avg_hired
ORDER BY h.hired DESC;

