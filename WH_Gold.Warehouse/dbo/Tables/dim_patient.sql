CREATE TABLE [dbo].[dim_patient] (
    [patient_key]  INT           NOT NULL,
    [patient_id]   VARCHAR (64)  NOT NULL,
    [family_name]  VARCHAR (500) NULL,
    [given_name]   VARCHAR (500) NULL,
    [gender]       VARCHAR (20)  NULL,
    [birth_date]   DATE          NULL,
    [city]         VARCHAR (200) NULL,
    [country]      VARCHAR (200) NULL,
    [last_updated] DATETIME2 (6) NULL,
    [dw_load_ts]   DATETIME2 (6) NULL
);


GO