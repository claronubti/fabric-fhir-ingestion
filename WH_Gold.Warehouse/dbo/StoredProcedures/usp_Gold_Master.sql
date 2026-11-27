CREATE   PROCEDURE dbo.usp_Gold_Master AS
BEGIN
    EXEC dbo.usp_Load_dim_date;
    EXEC dbo.usp_Load_dim_organization;
    EXEC dbo.usp_Load_dim_practitioner;
    EXEC dbo.usp_Load_dim_patient;
    EXEC dbo.usp_Load_fact_encounter;     -- before observation/condition: they look up encounter_key
    EXEC dbo.usp_Load_fact_observation;
    EXEC dbo.usp_Load_fact_condition;
END;

GO