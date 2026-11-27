CREATE TABLE [dbo].[ctl_resource_config] (
    [config_id]          INT           NOT NULL,
    [source_id]          INT           NULL,
    [resource_name]      VARCHAR (100) NULL,
    [endpoint]           VARCHAR (200) NULL,
    [primary_key]        VARCHAR (100) NULL,
    [incremental_column] VARCHAR (100) NULL,
    [incremental_param]  VARCHAR (100) NULL,
    [load_type]          VARCHAR (20)  NULL,
    [silver_table]       VARCHAR (100) NULL,
    [partition_column]   VARCHAR (100) NULL,
    [gold_proc]          VARCHAR (200) NULL,
    [load_order]         INT           NULL,
    [is_critical]        BIT           NULL,
    [is_active]          BIT           NULL
);


GO