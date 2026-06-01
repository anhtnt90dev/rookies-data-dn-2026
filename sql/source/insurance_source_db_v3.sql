/*
Task 115 - From-scratch database creation script

Purpose:
- Support incremental data loads from CRM source tables.
- Add created_date and updated_date to selected CRM tables directly in CREATE TABLE.
- Automatically set created_date on INSERT via DEFAULT SYSDATETIME().
- Automatically set updated_date on UPDATE via AFTER UPDATE triggers.

Note:
- SQL Server TIMESTAMP is ROWVERSION, not a date/time type.
- DATETIME2(3) is used for timestamp-style created/updated date fields.
*/

CREATE DATABASE insurance_crm_db;
GO

USE insurance_crm_db;
GO
-- =========================================
-- CUSTOMERS
-- =========================================
CREATE TABLE customers (
    customer_id         VARCHAR(20) PRIMARY KEY,
    full_name           NVARCHAR(200),
    gender              VARCHAR(10),
    dob                 DATE,
    phone_number        VARCHAR(20),
    email               VARCHAR(200),
    city                NVARCHAR(100),
    district            NVARCHAR(100),
    created_date        DATETIME2(3) NOT NULL
        CONSTRAINT DF_customers_created_date DEFAULT SYSDATETIME(),
    updated_date        DATETIME2(3) NULL
);
-- =========================================
-- AGENTS
-- =========================================
CREATE TABLE agents (
    agent_id            VARCHAR(20) PRIMARY KEY,
    agent_name          NVARCHAR(200),
    region              NVARCHAR(100),
    branch              NVARCHAR(100),
    manager_name        NVARCHAR(200),
    created_date        DATETIME2(3) NOT NULL
        CONSTRAINT DF_agents_created_date DEFAULT SYSDATETIME(),
    updated_date        DATETIME2(3) NULL
);

-- =========================================
-- INSURANCE PROVIDERS
-- =========================================
CREATE TABLE insurance_providers (
    provider_code       VARCHAR(20) PRIMARY KEY,
    provider_name       NVARCHAR(200),
    provider_group      NVARCHAR(100),
    active_flag         INT,
    created_date        DATETIME2(3) NOT NULL
        CONSTRAINT DF_insurance_providers_created_date DEFAULT SYSDATETIME(),
    updated_date        DATETIME2(3) NULL
);

-- =========================================
-- VEHICLE
-- =========================================
CREATE TABLE vehicle (
    vehicle_id          VARCHAR(20) PRIMARY KEY,
    customer_id         VARCHAR(20),
    plate_number        VARCHAR(20),
    vehicle_brand       NVARCHAR(100),
    vehicle_model       NVARCHAR(100),
    manufacture_year    INT,
    vehicle_value       DECIMAL(18,2),
    created_date        DATETIME2(3) NOT NULL
        CONSTRAINT DF_vehicle_created_date DEFAULT SYSDATETIME(),
    updated_date        DATETIME2(3) NULL,

    CONSTRAINT fk_vehicle_customer
        FOREIGN KEY (customer_id)
        REFERENCES customers(customer_id)
);

-- =========================================
-- QUOTATION
-- =========================================
CREATE TABLE quotation (
    quotation_id            VARCHAR(20) PRIMARY KEY,
    customer_id             VARCHAR(20),
    agent_id                VARCHAR(20),
    provider_code           VARCHAR(20),
    quotation_date          DATETIME,
    quotation_status        VARCHAR(50),
    package_code            VARCHAR(50),
    premium_amount          DECIMAL(18,2),
    quotation_expiry_date   DATETIME,
    created_date            DATETIME2(3) NOT NULL
        CONSTRAINT DF_quotation_created_date DEFAULT SYSDATETIME(),
    updated_date            DATETIME2(3) NULL,

    CONSTRAINT fk_quotation_customer
        FOREIGN KEY (customer_id)
        REFERENCES customers(customer_id),

    CONSTRAINT fk_quotation_agent
        FOREIGN KEY (agent_id)
        REFERENCES agents(agent_id),

    CONSTRAINT fk_quotation_provider
        FOREIGN KEY (provider_code)
        REFERENCES insurance_providers(provider_code)
);

-- =========================================
-- QUOTATION ITEM
-- =========================================
CREATE TABLE quotation_item (
    quotation_item_id       VARCHAR(20) PRIMARY KEY,
    quotation_id            VARCHAR(20),
    coverage_type           NVARCHAR(100),
    coverage_amount         DECIMAL(18,2),
    deductible_amount       DECIMAL(18,2),
    created_date            DATETIME2(3) NOT NULL
        CONSTRAINT DF_quotation_item_created_date DEFAULT SYSDATETIME(),
    updated_date            DATETIME2(3) NULL,

    CONSTRAINT fk_quotation_item
        FOREIGN KEY (quotation_id)
        REFERENCES quotation(quotation_id)
);

-- =========================================
-- CRM UPDATED_DATE TRIGGERS
-- =========================================
GO
CREATE TRIGGER trg_customers_set_updated_date
ON customers
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    IF (TRIGGER_NESTLEVEL() > 1)
        RETURN;

    UPDATE target
    SET updated_date = SYSDATETIME()
    FROM customers AS target
    INNER JOIN inserted AS i
        ON target.customer_id = i.customer_id;
END;

GO
CREATE TRIGGER trg_agents_set_updated_date
ON agents
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    IF (TRIGGER_NESTLEVEL() > 1)
        RETURN;

    UPDATE target
    SET updated_date = SYSDATETIME()
    FROM agents AS target
    INNER JOIN inserted AS i
        ON target.agent_id = i.agent_id;
END;
GO

CREATE TRIGGER trg_insurance_providers_set_updated_date
ON insurance_providers
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    IF (TRIGGER_NESTLEVEL() > 1)
        RETURN;

    UPDATE target
    SET updated_date = SYSDATETIME()
    FROM insurance_providers AS target
    INNER JOIN inserted AS i
        ON target.provider_code = i.provider_code;
END;
GO

CREATE TRIGGER trg_vehicle_set_updated_date
ON vehicle
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    IF (TRIGGER_NESTLEVEL() > 1)
        RETURN;

    UPDATE target
    SET updated_date = SYSDATETIME()
    FROM vehicle AS target
    INNER JOIN inserted AS i
        ON target.vehicle_id = i.vehicle_id;
END;
GO

CREATE TRIGGER trg_quotation_set_updated_date
ON quotation
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    IF (TRIGGER_NESTLEVEL() > 1)
        RETURN;

    UPDATE target
    SET updated_date = SYSDATETIME()
    FROM quotation AS target
    INNER JOIN inserted AS i
        ON target.quotation_id = i.quotation_id;
END;
GO

CREATE TRIGGER trg_quotation_item_set_updated_date
ON quotation_item
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    IF (TRIGGER_NESTLEVEL() > 1)
        RETURN;

    UPDATE target
    SET updated_date = SYSDATETIME()
    FROM quotation_item AS target
    INNER JOIN inserted AS i
        ON target.quotation_item_id = i.quotation_item_id;
END;
GO

USE insurance_crm_db;
GO

-- =========================================
-- INSURANCE PROVIDERS
-- =========================================
INSERT INTO insurance_providers (
    provider_code,
    provider_name,
    provider_group,
    active_flag
)
VALUES
('BV', 'Bao Viet', 'Domestic', 1),
('PVI', 'PVI Insurance', 'Domestic', 1),
('PTI', 'PTI Insurance', 'Domestic', 1),
('MIC', 'MIC Insurance', 'Domestic', 1),
('LIB', 'Liberty Insurance', 'International', 1),
('BIC', 'BIC Insurance', 'Domestic', 1);

-- =========================================
-- AGENTS
-- =========================================
INSERT INTO agents (
    agent_id,
    agent_name,
    region,
    branch,
    manager_name
)
VALUES
('AG001', 'Nguyen Van An', 'North', 'Ha Noi', 'Tran Minh'),
('AG002', 'Tran Thi Hoa', 'South', 'HCM', 'Le Anh'),
('AG003', 'Pham Minh Duc', 'Central', 'Da Nang', 'Nguyen Long'),
('AG004', 'Le Thi Mai', 'South', 'Can Tho', 'Le Anh');

-- Generate Initial Data for 3 Years
USE insurance_crm_db;
GO

DECLARE @i INT = 1;

WHILE @i <= 1000
BEGIN

    INSERT INTO customers (
        customer_id,
        full_name,
        gender,
        dob,
        phone_number,
        email,
        city,
        district
    )
    VALUES (
        CONCAT('CUS', RIGHT('0000' + CAST(@i AS VARCHAR), 4)),
        CONCAT('Customer ', @i),
        CASE WHEN @i % 2 = 0 THEN 'Male' ELSE 'Female' END,
        DATEADD(DAY, -(@i * 30), '1995-01-01'),
        CONCAT('090', RIGHT('0000000' + CAST(@i AS VARCHAR), 7)),
        CONCAT('customer', @i, '@mail.com'),
        CASE WHEN @i % 3 = 0 THEN 'Ha Noi'
             WHEN @i % 3 = 1 THEN 'Ho Chi Minh'
             ELSE 'Da Nang' END,
        'District 1'
    );

    INSERT INTO vehicle (
        vehicle_id,
        customer_id,
        plate_number,
        vehicle_brand,
        vehicle_model,
        manufacture_year,
        vehicle_value
    )
    VALUES (
        CONCAT('VEH', RIGHT('0000' + CAST(@i AS VARCHAR), 4)),
        CONCAT('CUS', RIGHT('0000' + CAST(@i AS VARCHAR), 4)),
        CONCAT('51A-', RIGHT('00000' + CAST(@i AS VARCHAR), 5)),
        CASE WHEN @i % 4 = 0 THEN 'Toyota'
             WHEN @i % 4 = 1 THEN 'Hyundai'
             WHEN @i % 4 = 2 THEN 'Mazda'
             ELSE 'VinFast' END,
        'Model X',
        2020 + (@i % 5),
        500000000 + (@i * 100000)
    );

    INSERT INTO quotation (
        quotation_id,
        customer_id,
        agent_id,
        provider_code,
        quotation_date,
        quotation_status,
        package_code,
        premium_amount,
        quotation_expiry_date
    )
    VALUES (
        CONCAT('QUO', RIGHT('00000' + CAST(@i AS VARCHAR), 5)),
        CONCAT('CUS', RIGHT('0000' + CAST(@i AS VARCHAR), 4)),
        CASE WHEN @i % 4 = 0 THEN 'AG001'
             WHEN @i % 4 = 1 THEN 'AG002'
             WHEN @i % 4 = 2 THEN 'AG003'
             ELSE 'AG004' END,
        CASE WHEN @i % 5 = 0 THEN 'BV'
             WHEN @i % 5 = 1 THEN 'PVI'
             WHEN @i % 5 = 2 THEN 'PTI'
             WHEN @i % 5 = 3 THEN 'MIC'
             ELSE 'LIB' END,
        DATEADD(DAY, -(@i % 1095), GETDATE()),
        CASE WHEN @i % 5 = 0 THEN 'CONVERTED'
             WHEN @i % 5 = 1 THEN 'ACCEPTED'
             WHEN @i % 5 = 2 THEN 'REJECTED'
             WHEN @i % 5 = 3 THEN 'EXPIRED'
             ELSE 'QUOTED' END,
        CASE WHEN @i % 4 = 0 THEN 'BASIC'
             WHEN @i % 4 = 1 THEN 'STANDARD'
             WHEN @i % 4 = 2 THEN 'PREMIUM'
             ELSE 'VIP' END,
        5000000 + (@i * 10000),
        DATEADD(DAY, 30, GETDATE())
    );

    INSERT INTO quotation_item (
        quotation_item_id,
        quotation_id,
        coverage_type,
        coverage_amount,
        deductible_amount
    )
    VALUES (
        CONCAT('QI', RIGHT('00000' + CAST(@i AS VARCHAR), 5)),
        CONCAT('QUO', RIGHT('00000' + CAST(@i AS VARCHAR), 5)),
        'Physical Damage',
        100000000,
        1000000
    );

    SET @i = @i + 1;
END;


CREATE DATABASE insurance_policy_db;
GO

USE insurance_policy_db;
GO

-- =========================================
-- POLICY
-- =========================================
CREATE TABLE policy_info (
    policy_id               VARCHAR(20) PRIMARY KEY,
    quotation_id            VARCHAR(20),
    customer_id             VARCHAR(20),
    provider_code           VARCHAR(20),
    policy_number           VARCHAR(50),
    policy_start_date       DATE,
    policy_end_date         DATE,
    policy_status           VARCHAR(50),
    premium_amount          DECIMAL(18,2),
    issued_date             DATETIME
);

-- =========================================
-- PAYMENT
-- =========================================
CREATE TABLE payment (
    payment_id              VARCHAR(20) PRIMARY KEY,
    policy_id               VARCHAR(20),
    payment_date            DATETIME,
    payment_method          VARCHAR(50),
    payment_status          VARCHAR(50),
    payment_amount          DECIMAL(18,2),
    transaction_reference   VARCHAR(100),

    CONSTRAINT fk_payment_policy
        FOREIGN KEY (policy_id)
        REFERENCES policy_info(policy_id)
);

-- =========================================
-- CANCELLATION
-- =========================================
CREATE TABLE cancellation (
    cancellation_id         VARCHAR(20) PRIMARY KEY,
    policy_id               VARCHAR(20),
    cancellation_date       DATETIME,
    cancellation_reason     NVARCHAR(200),
    refund_amount           DECIMAL(18,2),

    CONSTRAINT fk_cancel_policy
        FOREIGN KEY (policy_id)
        REFERENCES policy_info(policy_id)
);

USE insurance_policy_db;
GO

DECLARE @i INT = 1;

WHILE @i <= 600
BEGIN

    INSERT INTO policy_info
    VALUES (
        CONCAT('POL', RIGHT('00000' + CAST(@i AS VARCHAR), 5)),
        CONCAT('QUO', RIGHT('00000' + CAST(@i AS VARCHAR), 5)),
        CONCAT('CUS', RIGHT('0000' + CAST(@i AS VARCHAR), 4)),
        CASE WHEN @i % 5 = 0 THEN 'BV'
             WHEN @i % 5 = 1 THEN 'PVI'
             WHEN @i % 5 = 2 THEN 'PTI'
             WHEN @i % 5 = 3 THEN 'MIC'
             ELSE 'LIB' END,
        CONCAT('POLNO-', @i),
        DATEADD(DAY, -(@i % 1095), GETDATE()),
        DATEADD(YEAR, 1, DATEADD(DAY, -(@i % 1095), GETDATE())),
        CASE WHEN @i % 4 = 0 THEN 'ACTIVE'
             WHEN @i % 4 = 1 THEN 'EXPIRED'
             WHEN @i % 4 = 2 THEN 'CANCELLED'
             ELSE 'ISSUED' END,
        6000000 + (@i * 15000),
        DATEADD(DAY, -(@i % 1095), GETDATE())
    );

    INSERT INTO payment
    VALUES (
        CONCAT('PAY', RIGHT('00000' + CAST(@i AS VARCHAR), 5)),
        CONCAT('POL', RIGHT('00000' + CAST(@i AS VARCHAR), 5)),
        DATEADD(DAY, -(@i % 1095), GETDATE()),
        CASE WHEN @i % 3 = 0 THEN 'Bank Transfer'
             WHEN @i % 3 = 1 THEN 'Credit Card'
             ELSE 'E-wallet' END,
        CASE WHEN @i % 4 = 0 THEN 'PAID'
             WHEN @i % 4 = 1 THEN 'FAILED'
             WHEN @i % 4 = 2 THEN 'PENDING'
             ELSE 'REFUNDED' END,
        6000000 + (@i * 15000),
        CONCAT('TXN', @i)
    );

    IF @i % 10 = 0
    BEGIN
        INSERT INTO cancellation
        VALUES (
            CONCAT('CAN', RIGHT('00000' + CAST(@i AS VARCHAR), 5)),
            CONCAT('POL', RIGHT('00000' + CAST(@i AS VARCHAR), 5)),
            GETDATE(),
            'Customer Request',
            1000000
        );
    END

    SET @i = @i + 1;
END;

