CREATE   PROCEDURE dbo.usp_Load_dim_date AS
BEGIN
    SET NOCOUNT ON;
    -- 10,000 consecutive days from 2000-01-01 (no recursive CTE needed)
    ;WITH d AS (SELECT n FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) v(n)),
    nums AS (
        SELECT a.n + 10*b.n + 100*c.n + 1000*e.n AS n
        FROM d a CROSS JOIN d b CROSS JOIN d c CROSS JOIN d e
    ),
    dates AS (SELECT DATEADD(DAY, n, CAST('2000-01-01' AS DATE)) AS dt FROM nums)
    INSERT INTO dbo.dim_date (date_key, full_date, [year], [quarter], [month], month_name, [day], day_of_week, is_weekend)
    SELECT YEAR(dt)*10000 + MONTH(dt)*100 + DAY(dt), dt, YEAR(dt), DATEPART(QUARTER, dt), MONTH(dt),
           DATENAME(MONTH, dt), DAY(dt), DATEPART(WEEKDAY, dt),
           CASE WHEN DATEPART(WEEKDAY, dt) IN (1, 7) THEN 1 ELSE 0 END
    FROM dates x
    WHERE NOT EXISTS (SELECT 1 FROM dbo.dim_date t WHERE t.date_key = YEAR(x.dt)*10000 + MONTH(x.dt)*100 + DAY(x.dt));
END;

GO