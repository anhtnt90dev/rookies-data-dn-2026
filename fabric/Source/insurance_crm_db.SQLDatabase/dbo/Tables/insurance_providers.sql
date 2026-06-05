CREATE TABLE [dbo].[insurance_providers] (
    [provider_code]  VARCHAR (20)   NOT NULL,
    [provider_name]  NVARCHAR (200) NULL,
    [provider_group] NVARCHAR (100) NULL,
    [active_flag]    INT            NULL,
    [created_date]   DATETIME2 (3)  CONSTRAINT [DF_insurance_providers_created_date] DEFAULT (sysdatetime()) NOT NULL,
    [updated_date]   DATETIME2 (3)  NULL,
    PRIMARY KEY CLUSTERED ([provider_code] ASC)
);


GO


CREATE TRIGGER dbo.trg_insurance_providers_set_updated_date
ON dbo.insurance_providers
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    IF (TRIGGER_NESTLEVEL() > 1)
        RETURN;

    UPDATE target
    SET updated_date = SYSDATETIME()
    FROM dbo.insurance_providers AS target
    INNER JOIN inserted AS i
        ON target.provider_code = i.provider_code;
END;

GO

