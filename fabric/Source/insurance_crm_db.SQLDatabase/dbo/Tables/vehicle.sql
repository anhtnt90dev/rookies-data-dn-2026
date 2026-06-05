CREATE TABLE [dbo].[vehicle] (
    [vehicle_id]       VARCHAR (20)    NOT NULL,
    [customer_id]      VARCHAR (20)    NULL,
    [plate_number]     VARCHAR (20)    NULL,
    [vehicle_brand]    NVARCHAR (100)  NULL,
    [vehicle_model]    NVARCHAR (100)  NULL,
    [manufacture_year] INT             NULL,
    [vehicle_value]    DECIMAL (18, 2) NULL,
    [created_date]     DATETIME2 (3)   CONSTRAINT [DF_vehicle_created_date] DEFAULT (sysdatetime()) NOT NULL,
    [updated_date]     DATETIME2 (3)   NULL,
    PRIMARY KEY CLUSTERED ([vehicle_id] ASC),
    CONSTRAINT [fk_vehicle_customer] FOREIGN KEY ([customer_id]) REFERENCES [dbo].[customers] ([customer_id])
);


GO


CREATE TRIGGER dbo.trg_vehicle_set_updated_date
ON dbo.vehicle
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    IF (TRIGGER_NESTLEVEL() > 1)
        RETURN;

    UPDATE target
    SET updated_date = SYSDATETIME()
    FROM dbo.vehicle AS target
    INNER JOIN inserted AS i
        ON target.vehicle_id = i.vehicle_id;
END;

GO

