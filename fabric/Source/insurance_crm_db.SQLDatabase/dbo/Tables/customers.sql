CREATE TABLE [dbo].[customers] (
    [customer_id]  VARCHAR (20)   NOT NULL,
    [full_name]    NVARCHAR (200) NULL,
    [gender]       VARCHAR (10)   NULL,
    [dob]          DATE           NULL,
    [phone_number] VARCHAR (20)   NULL,
    [email]        VARCHAR (200)  NULL,
    [city]         NVARCHAR (100) NULL,
    [district]     NVARCHAR (100) NULL,
    [created_date] DATETIME2 (3)  CONSTRAINT [DF_customers_created_date] DEFAULT (sysdatetime()) NOT NULL,
    [updated_date] DATETIME2 (3)  NULL,
    PRIMARY KEY CLUSTERED ([customer_id] ASC)
);


GO


CREATE TRIGGER dbo.trg_customers_set_updated_date
ON dbo.customers
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    IF (TRIGGER_NESTLEVEL() > 1)
        RETURN;

    UPDATE target
    SET updated_date = SYSDATETIME()
    FROM dbo.customers AS target
    INNER JOIN inserted AS i
        ON target.customer_id = i.customer_id;
END;

GO

