CREATE TABLE [dbo].[ctl_api_source] (
    [source_id]   INT           NOT NULL,
    [source_name] VARCHAR (100) NULL,
    [base_url]    VARCHAR (500) NULL,
    [auth_type]   VARCHAR (50)  NULL,
    [page_size]   INT           NULL,
    [is_active]   BIT           NULL
);


GO