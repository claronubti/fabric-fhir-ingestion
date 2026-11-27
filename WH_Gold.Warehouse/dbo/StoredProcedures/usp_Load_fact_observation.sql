CREATE   PROCEDURE dbo.usp_Load_fact_observation AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @max INT;
    SELECT @max = ISNULL(MAX(observation_key), 0) FROM dbo.fact_observation WHERE observation_key > 0;

    UPDATE f
    SET patient_key   = ISNULL(p.patient_key, -1),
        encounter_key = ISNULL(e.encounter_key, -1),
        effective_date_key = ISNULL(YEAR(s.effective_ts)*10000 + MONTH(s.effective_ts)*100 + DAY(s.effective_ts), -1),
        status = LEFT(s.status, 50), code_system = LEFT(s.code_system, 500), code = LEFT(s.code, 200),
        code_display = LEFT(s.code_display, 500), value_numeric = CAST(s.value_numeric AS DECIMAL(18,4)),
        value_unit = LEFT(s.value_unit, 100), value_string = LEFT(s.value_string, 1000),
        effective_ts = s.effective_ts, last_updated = s.lastUpdated, dw_load_ts = SYSUTCDATETIME()
    FROM dbo.fact_observation f
    JOIN LH_Silver.dbo.silver_observation s ON s.id = f.observation_id
    LEFT JOIN dbo.dim_patient p     ON p.patient_id = s.patient_id
    LEFT JOIN dbo.fact_encounter e  ON e.encounter_id = s.encounter_id
    WHERE s.lastUpdated > f.last_updated OR f.last_updated IS NULL;

    INSERT INTO dbo.fact_observation
        (observation_key, observation_id, patient_key, encounter_key, effective_date_key, status, code_system, code,
         code_display, value_numeric, value_unit, value_string, effective_ts, last_updated, dw_load_ts)
    SELECT @max + ROW_NUMBER() OVER (ORDER BY s.id), s.id, ISNULL(p.patient_key, -1), ISNULL(e.encounter_key, -1),
           ISNULL(YEAR(s.effective_ts)*10000 + MONTH(s.effective_ts)*100 + DAY(s.effective_ts), -1),
           LEFT(s.status, 50), LEFT(s.code_system, 500), LEFT(s.code, 200), LEFT(s.code_display, 500),
           CAST(s.value_numeric AS DECIMAL(18,4)), LEFT(s.value_unit, 100), LEFT(s.value_string, 1000),
           s.effective_ts, s.lastUpdated, SYSUTCDATETIME()
    FROM LH_Silver.dbo.silver_observation s
    LEFT JOIN dbo.dim_patient p     ON p.patient_id = s.patient_id
    LEFT JOIN dbo.fact_encounter e  ON e.encounter_id = s.encounter_id
    WHERE NOT EXISTS (SELECT 1 FROM dbo.fact_observation f WHERE f.observation_id = s.id);
END;

GO