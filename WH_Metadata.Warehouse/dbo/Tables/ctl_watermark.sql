CREATE TABLE [dbo].[ctl_watermark] (
    [config_id]           INT           NOT NULL,
    [last_watermark]      DATETIME2 (6) NULL,
    [last_success_run_id] VARCHAR (50)  NULL,
    [updated_at]          DATETIME2 (6) NULL
);


GO