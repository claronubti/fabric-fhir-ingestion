CREATE   PROCEDURE dbo.usp_Load_dim_practitioner AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @max INT;
    SELECT @max = ISNULL(MAX(practitioner_key), 0) FROM dbo.dim_practitioner WHERE practitioner_key > 0;

    UPDATE d
    SET family_name = LEFT(s.family, 500), given_name = LEFT(s.given, 500), gender = LEFT(s.gender, 20),
        last_updated = s.lastUpdated, dw_load_ts = SYSUTCDATETIME()
    FROM dbo.dim_practitioner d
    JOIN LH_Silver.dbo.silver_practitioner s ON s.id = d.practitioner_id
    WHERE s.lastUpdated > d.last_updated OR d.last_updated IS NULL;

    INSERT INTO dbo.dim_practitioner (practitioner_key, practitioner_id, family_name, given_name, gender, last_updated, dw_load_ts)
    SELECT @max + ROW_NUMBER() OVER (ORDER BY s.id), s.id, LEFT(s.family, 500), LEFT(s.given, 500),
           LEFT(s.gender, 20), s.lastUpdated, SYSUTCDATETIME()
    FROM LH_Silver.dbo.silver_practitioner s
    WHERE NOT EXISTS (SELECT 1 FROM dbo.dim_practitioner d WHERE d.practitioner_id = s.id);
END;

GO