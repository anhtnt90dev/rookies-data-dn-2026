CREATE TABLE [dbo].[quotation_item] (
    [quotation_item_id] VARCHAR (20)    NOT NULL,
    [quotation_id]      VARCHAR (20)    NULL,
    [coverage_type]     NVARCHAR (100)  NULL,
    [coverage_amount]   DECIMAL (18, 2) NULL,
    [deductible_amount] DECIMAL (18, 2) NULL,
    [created_date]      DATETIME2 (3)   CONSTRAINT [DF_quotation_item_created_date] DEFAULT (sysdatetime()) NOT NULL,
    [updated_date]      DATETIME2 (3)   NULL,
    PRIMARY KEY CLUSTERED ([quotation_item_id] ASC),
    CONSTRAINT [fk_quotation_item] FOREIGN KEY ([quotation_id]) REFERENCES [dbo].[quotation] ([quotation_id])
);


GO


CREATE TRIGGER dbo.trg_quotation_item_set_updated_date
ON dbo.quotation_item
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    IF (TRIGGER_NESTLEVEL() > 1)
        RETURN;

    UPDATE target
    SET updated_date = SYSDATETIME()
    FROM dbo.quotation_item AS target
    INNER JOIN inserted AS i
        ON target.quotation_item_id = i.quotation_item_id;
END;

GO

