CREATE TABLE [dbo].[dim_date] (
    [date_key]    INT          NOT NULL,
    [full_date]   DATE         NULL,
    [year]        INT          NULL,
    [quarter]     INT          NULL,
    [month]       INT          NULL,
    [month_name]  VARCHAR (20) NULL,
    [day]         INT          NULL,
    [day_of_week] INT          NULL,
    [is_weekend]  BIT          NULL
);


GO