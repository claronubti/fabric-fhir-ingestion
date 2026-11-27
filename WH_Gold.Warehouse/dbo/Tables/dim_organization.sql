CREATE TABLE [dbo].[dim_organization] (
    [organization_key] INT           NOT NULL,
    [organization_id]  VARCHAR (64)  NOT NULL,
    [name]             VARCHAR (500) NULL,
    [active]           BIT           NULL,
    [city]             VARCHAR (200) NULL,
    [country]          VARCHAR (200) NULL,
    [last_updated]     DATETIME2 (6) NULL,
    [dw_load_ts]       DATETIME2 (6) NULL
);


GO