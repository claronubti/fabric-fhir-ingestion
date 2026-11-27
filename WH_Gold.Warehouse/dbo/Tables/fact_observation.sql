CREATE TABLE [dbo].[fact_observation] (
    [observation_key]    INT             NOT NULL,
    [observation_id]     VARCHAR (64)    NOT NULL,
    [patient_key]        INT             NULL,
    [encounter_key]      INT             NULL,
    [effective_date_key] INT             NULL,
    [status]             VARCHAR (50)    NULL,
    [code_system]        VARCHAR (500)   NULL,
    [code]               VARCHAR (200)   NULL,
    [code_display]       VARCHAR (500)   NULL,
    [value_numeric]      DECIMAL (18, 4) NULL,
    [value_unit]         VARCHAR (100)   NULL,
    [value_string]       VARCHAR (1000)  NULL,
    [effective_ts]       DATETIME2 (6)   NULL,
    [last_updated]       DATETIME2 (6)   NULL,
    [dw_load_ts]         DATETIME2 (6)   NULL
);


GO