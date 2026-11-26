CREATE TABLE [dbo].[ctl_schema_registry] (
    [config_id]   INT           NULL,
    [column_path] VARCHAR (500) NULL,
    [data_type]   VARCHAR (50)  NULL,
    [first_seen]  DATETIME2 (6) NULL,
    [last_seen]   DATETIME2 (6) NULL,
    [is_active]   BIT           NULL
);


GO