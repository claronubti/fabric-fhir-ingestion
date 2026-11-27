CREATE   PROCEDURE dbo.usp_Load_dim_organization AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @max INT;
    SELECT @max = ISNULL(MAX(organization_key), 0) FROM dbo.dim_organization WHERE organization_key > 0;

    UPDATE d
    SET name = LEFT(s.name, 500), active = s.active, city = LEFT(s.city, 200), country = LEFT(s.country, 200),
        last_updated = s.lastUpdated, dw_load_ts = SYSUTCDATETIME()
    FROM dbo.dim_organization d
    JOIN LH_Silver.dbo.silver_organization s ON s.id = d.organization_id
    WHERE s.lastUpdated > d.last_updated OR d.last_updated IS NULL;

    INSERT INTO dbo.dim_organization (organization_key, organization_id, name, active, city, country, last_updated, dw_load_ts)
    SELECT @max + ROW_NUMBER() OVER (ORDER BY s.id), s.id, LEFT(s.name, 500), s.active,
           LEFT(s.city, 200), LEFT(s.country, 200), s.lastUpdated, SYSUTCDATETIME()
    FROM LH_Silver.dbo.silver_organization s
    WHERE NOT EXISTS (SELECT 1 FROM dbo.dim_organization d WHERE d.organization_id = s.id);
END;

GO