CREATE   PROCEDURE dbo.usp_Load_dim_patient AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @max INT;
    SELECT @max = ISNULL(MAX(patient_key), 0) FROM dbo.dim_patient WHERE patient_key > 0;

    UPDATE d
    SET family_name = LEFT(s.family, 500), given_name = LEFT(s.given, 500), gender = LEFT(s.gender, 20),
        birth_date = s.birthDate, city = LEFT(s.city, 200), country = LEFT(s.country, 200),
        last_updated = s.lastUpdated, dw_load_ts = SYSUTCDATETIME()
    FROM dbo.dim_patient d
    JOIN LH_Silver.dbo.silver_patient s ON s.id = d.patient_id
    WHERE s.lastUpdated > d.last_updated OR d.last_updated IS NULL;

    INSERT INTO dbo.dim_patient (patient_key, patient_id, family_name, given_name, gender, birth_date, city, country, last_updated, dw_load_ts)
    SELECT @max + ROW_NUMBER() OVER (ORDER BY s.id), s.id, LEFT(s.family, 500), LEFT(s.given, 500), LEFT(s.gender, 20),
           s.birthDate, LEFT(s.city, 200), LEFT(s.country, 200), s.lastUpdated, SYSUTCDATETIME()
    FROM LH_Silver.dbo.silver_patient s
    WHERE NOT EXISTS (SELECT 1 FROM dbo.dim_patient d WHERE d.patient_id = s.id);
END;

GO