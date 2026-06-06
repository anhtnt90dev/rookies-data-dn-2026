CREATE TABLE [dbo].[quotation] (
    [quotation_id]          VARCHAR (20)    NOT NULL,
    [customer_id]           VARCHAR (20)    NULL,
    [agent_id]              VARCHAR (20)    NULL,
    [provider_code]         VARCHAR (20)    NULL,
    [quotation_date]        DATETIME        NULL,
    [quotation_status]      VARCHAR (50)    NULL,
    [package_code]          VARCHAR (50)    NULL,
    [premium_amount]        DECIMAL (18, 2) NULL,
    [quotation_expiry_date] DATETIME        NULL,
    [created_date]          DATETIME2 (3)   CONSTRAINT [DF_quotation_created_date] DEFAULT (sysdatetime()) NOT NULL,
    [updated_date]          DATETIME2 (3)   NULL,
    PRIMARY KEY CLUSTERED ([quotation_id] ASC),
    CONSTRAINT [fk_quotation_agent] FOREIGN KEY ([agent_id]) REFERENCES [dbo].[agents] ([agent_id]),
    CONSTRAINT [fk_quotation_customer] FOREIGN KEY ([customer_id]) REFERENCES [dbo].[customers] ([customer_id]),
    CONSTRAINT [fk_quotation_provider] FOREIGN KEY ([provider_code]) REFERENCES [dbo].[insurance_providers] ([provider_code])
);


GO


CREATE TRIGGER dbo.trg_quotation_set_updated_date
ON dbo.quotation
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    IF (TRIGGER_NESTLEVEL() > 1)
        RETURN;

    UPDATE target
    SET updated_date = SYSDATETIME()
    FROM dbo.quotation AS target
    INNER JOIN inserted AS i
        ON target.quotation_id = i.quotation_id;
END;

GO

