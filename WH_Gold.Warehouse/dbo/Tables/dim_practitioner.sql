CREATE TABLE [dbo].[dim_practitioner] (
    [practitioner_key] INT           NOT NULL,
    [practitioner_id]  VARCHAR (64)  NOT NULL,
    [family_name]      VARCHAR (500) NULL,
    [given_name]       VARCHAR (500) NULL,
    [gender]           VARCHAR (20)  NULL,
    [last_updated]     DATETIME2 (6) NULL,
    [dw_load_ts]       DATETIME2 (6) NULL
);


GO