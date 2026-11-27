CREATE   PROCEDURE dbo.usp_Load_fact_condition AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @max INT;
    SELECT @max = ISNULL(MAX(condition_key), 0) FROM dbo.fact_condition WHERE condition_key > 0;

    UPDATE f
    SET patient_key   = ISNULL(p.patient_key, -1),
        encounter_key = ISNULL(e.encounter_key, -1),
        clinical_status = LEFT(s.clinical_status, 50), verification_status = LEFT(s.verification_status, 50),
        code_system = LEFT(s.code_system, 500), code = LEFT(s.code, 200), code_display = LEFT(s.code_display, 500),
        onset_date_key    = ISNULL(YEAR(s.onset_ts)*10000 + MONTH(s.onset_ts)*100 + DAY(s.onset_ts), -1),
        recorded_date_key = ISNULL(YEAR(s.recorded_ts)*10000 + MONTH(s.recorded_ts)*100 + DAY(s.recorded_ts), -1),
        onset_ts = s.onset_ts, recorded_ts = s.recorded_ts, last_updated = s.lastUpdated, dw_load_ts = SYSUTCDATETIME()
    FROM dbo.fact_condition f
    JOIN LH_Silver.dbo.silver_condition s ON s.id = f.condition_id
    LEFT JOIN dbo.dim_patient p     ON p.patient_id = s.patient_id
    LEFT JOIN dbo.fact_encounter e  ON e.encounter_id = s.encounter_id
    WHERE s.lastUpdated > f.last_updated OR f.last_updated IS NULL;

    INSERT INTO dbo.fact_condition
        (condition_key, condition_id, patient_key, encounter_key, clinical_status, verification_status, code_system, code,
         code_display, onset_date_key, recorded_date_key, onset_ts, recorded_ts, last_updated, dw_load_ts)
    SELECT @max + ROW_NUMBER() OVER (ORDER BY s.id), s.id, ISNULL(p.patient_key, -1), ISNULL(e.encounter_key, -1),
           LEFT(s.clinical_status, 50), LEFT(s.verification_status, 50), LEFT(s.code_system, 500), LEFT(s.code, 200),
           LEFT(s.code_display, 500),
           ISNULL(YEAR(s.onset_ts)*10000 + MONTH(s.onset_ts)*100 + DAY(s.onset_ts), -1),
           ISNULL(YEAR(s.recorded_ts)*10000 + MONTH(s.recorded_ts)*100 + DAY(s.recorded_ts), -1),
           s.onset_ts, s.recorded_ts, s.lastUpdated, SYSUTCDATETIME()
    FROM LH_Silver.dbo.silver_condition s
    LEFT JOIN dbo.dim_patient p     ON p.patient_id = s.patient_id
    LEFT JOIN dbo.fact_encounter e  ON e.encounter_id = s.encounter_id
    WHERE NOT EXISTS (SELECT 1 FROM dbo.fact_condition f WHERE f.condition_id = s.id);
END;

GO