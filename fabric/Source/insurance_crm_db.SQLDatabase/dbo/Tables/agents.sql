CREATE TABLE [dbo].[agents] (
    [agent_id]     VARCHAR (20)   NOT NULL,
    [agent_name]   NVARCHAR (200) NULL,
    [region]       NVARCHAR (100) NULL,
    [branch]       NVARCHAR (100) NULL,
    [manager_name] NVARCHAR (200) NULL,
    [created_date] DATETIME2 (3)  CONSTRAINT [DF_agents_created_date] DEFAULT (sysdatetime()) NOT NULL,
    [updated_date] DATETIME2 (3)  NULL,
    PRIMARY KEY CLUSTERED ([agent_id] ASC)
);


GO


CREATE TRIGGER dbo.trg_agents_set_updated_date
ON dbo.agents
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    IF (TRIGGER_NESTLEVEL() > 1)
        RETURN;

    UPDATE target
    SET updated_date = SYSDATETIME()
    FROM dbo.agents AS target
    INNER JOIN inserted AS i
        ON target.agent_id = i.agent_id;
END;

GO

