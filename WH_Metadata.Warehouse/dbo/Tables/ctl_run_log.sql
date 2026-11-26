CREATE TABLE [dbo].[ctl_run_log] (
    [run_id]          VARCHAR (50)   NULL,
    [pipeline_run_id] VARCHAR (100)  NULL,
    [config_id]       INT            NULL,
    [layer]           VARCHAR (20)   NULL,
    [status]          VARCHAR (20)   NULL,
    [rows_read]       BIGINT         NULL,
    [rows_written]    BIGINT         NULL,
    [start_time]      DATETIME2 (6)  NULL,
    [end_time]        DATETIME2 (6)  NULL,
    [error_message]   VARCHAR (4000) NULL
);


GO