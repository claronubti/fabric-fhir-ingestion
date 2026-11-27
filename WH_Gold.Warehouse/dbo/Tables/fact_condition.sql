CREATE TABLE [dbo].[fact_condition] (
    [condition_key]       INT           NOT NULL,
    [condition_id]        VARCHAR (64)  NOT NULL,
    [patient_key]         INT           NULL,
    [encounter_key]       INT           NULL,
    [clinical_status]     VARCHAR (50)  NULL,
    [verification_status] VARCHAR (50)  NULL,
    [code_system]         VARCHAR (500) NULL,
    [code]                VARCHAR (200) NULL,
    [code_display]        VARCHAR (500) NULL,
    [onset_date_key]      INT           NULL,
    [recorded_date_key]   INT           NULL,
    [onset_ts]            DATETIME2 (6) NULL,
    [recorded_ts]         DATETIME2 (6) NULL,
    [last_updated]        DATETIME2 (6) NULL,
    [dw_load_ts]          DATETIME2 (6) NULL
);


GO