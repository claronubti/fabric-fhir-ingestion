CREATE   PROCEDURE dbo.usp_Load_fact_encounter AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @max INT;
    SELECT @max = ISNULL(MAX(encounter_key), 0) FROM dbo.fact_encounter WHERE encounter_key > 0;

    UPDATE f
    SET patient_key      = ISNULL(p.patient_key, -1),
        practitioner_key = ISNULL(pr.practitioner_key, -1),
        organization_key = ISNULL(o.organization_key, -1),
        status = LEFT(s.status, 50), class_code = LEFT(s.class_code, 50), class_display = LEFT(s.class_display, 200),
        start_date_key = ISNULL(YEAR(s.period_start)*10000 + MONTH(s.period_start)*100 + DAY(s.period_start), -1),
        end_date_key   = ISNULL(YEAR(s.period_end)*10000 + MONTH(s.period_end)*100 + DAY(s.period_end), -1),
        period_start = s.period_start, period_end = s.period_end,
        last_updated = s.lastUpdated, dw_load_ts = SYSUTCDATETIME()
    FROM dbo.fact_encounter f
    JOIN LH_Silver.dbo.silver_encounter s ON s.id = f.encounter_id
    LEFT JOIN dbo.dim_patient p        ON p.patient_id = s.patient_id
    LEFT JOIN dbo.dim_practitioner pr  ON pr.practitioner_id = s.practitioner_id
    LEFT JOIN dbo.dim_organization o   ON o.organization_id = s.organization_id
    WHERE s.lastUpdated > f.last_updated OR f.last_updated IS NULL;

    INSERT INTO dbo.fact_encounter
        (encounter_key, encounter_id, patient_key, practitioner_key, organization_key, status, class_code, class_display,
         start_date_key, end_date_key, period_start, period_end, last_updated, dw_load_ts)
    SELECT @max + ROW_NUMBER() OVER (ORDER BY s.id), s.id,
           ISNULL(p.patient_key, -1), ISNULL(pr.practitioner_key, -1), ISNULL(o.organization_key, -1),
           LEFT(s.status, 50), LEFT(s.class_code, 50), LEFT(s.class_display, 200),
           ISNULL(YEAR(s.period_start)*10000 + MONTH(s.period_start)*100 + DAY(s.period_start), -1),
           ISNULL(YEAR(s.period_end)*10000 + MONTH(s.period_end)*100 + DAY(s.period_end), -1),
           s.period_start, s.period_end, s.lastUpdated, SYSUTCDATETIME()
    FROM LH_Silver.dbo.silver_encounter s
    LEFT JOIN dbo.dim_patient p        ON p.patient_id = s.patient_id
    LEFT JOIN dbo.dim_practitioner pr  ON pr.practitioner_id = s.practitioner_id
    LEFT JOIN dbo.dim_organization o   ON o.organization_id = s.organization_id
    WHERE NOT EXISTS (SELECT 1 FROM dbo.fact_encounter f WHERE f.encounter_id = s.id);
END;

GO