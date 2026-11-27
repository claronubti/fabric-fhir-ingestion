CREATE TABLE [dbo].[fact_encounter] (
    [encounter_key]    INT           NOT NULL,
    [encounter_id]     VARCHAR (64)  NOT NULL,
    [patient_key]      INT           NULL,
    [practitioner_key] INT           NULL,
    [organization_key] INT           NULL,
    [status]           VARCHAR (50)  NULL,
    [class_code]       VARCHAR (50)  NULL,
    [class_display]    VARCHAR (200) NULL,
    [start_date_key]   INT           NULL,
    [end_date_key]     INT           NULL,
    [period_start]     DATETIME2 (6) NULL,
    [period_end]       DATETIME2 (6) NULL,
    [last_updated]     DATETIME2 (6) NULL,
    [dw_load_ts]       DATETIME2 (6) NULL
);


GO