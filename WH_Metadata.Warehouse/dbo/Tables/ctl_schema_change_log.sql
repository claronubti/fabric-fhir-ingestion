CREATE TABLE [dbo].[ctl_schema_change_log] (
    [change_id]    BIGINT        NULL,
    [config_id]    INT           NULL,
    [column_path]  VARCHAR (500) NULL,
    [change_type]  VARCHAR (30)  NULL,
    [old_type]     VARCHAR (50)  NULL,
    [new_type]     VARCHAR (50)  NULL,
    [detected_at]  DATETIME2 (6) NULL,
    [action_taken] VARCHAR (100) NULL
);


GO