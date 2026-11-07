-- Ported from code_00_03_Spark_BDE_Setup_Prerequisites.ipynb ("Setup MariaDB")
-- Runs once, as root, on first container start (empty mysql-data volume).
-- The `spark` user already exists at this point (MYSQL_USER / MYSQL_PASSWORD).

-- Local warehouse stock (source for the batch upload job)
CREATE DATABASE IF NOT EXISTS warehouse_stock;
CREATE TABLE IF NOT EXISTS warehouse_stock.item_stock (
    ID             INT           NOT NULL AUTO_INCREMENT,
    STOCK_DATE     DATETIME      NOT NULL,
    WAREHOUSE_ID   VARCHAR(45)   NOT NULL,
    ITEM_NAME      VARCHAR(45)   NOT NULL,
    OPENING_STOCK  INT           NOT NULL DEFAULT 0,
    RECEIPTS       INT           NOT NULL DEFAULT 0,
    ISSUES         INT           NOT NULL DEFAULT 0,
    UNIT_VALUE     DECIMAL(10,2) NOT NULL DEFAULT 0,
    PRIMARY KEY (ID),
    INDEX STOCK_DATE (STOCK_DATE ASC)
);

-- Global aggregated stock (sink for the batch aggregation job)
CREATE DATABASE IF NOT EXISTS global_stock;
CREATE TABLE IF NOT EXISTS global_stock.item_stock (
    ID             INT           NOT NULL AUTO_INCREMENT,
    STOCK_DATE     DATETIME      NOT NULL,
    ITEM_NAME      VARCHAR(45)   NOT NULL,
    TOTAL_REC      INT           NOT NULL DEFAULT 0,
    OPENING_STOCK  INT           NOT NULL DEFAULT 0,
    RECEIPTS       INT           NOT NULL DEFAULT 0,
    ISSUES         INT           NOT NULL DEFAULT 0,
    CLOSING_STOCK  INT           NOT NULL DEFAULT 0,
    CLOSING_VALUE  DECIMAL(10,2) NOT NULL DEFAULT 0,
    PRIMARY KEY (ID),
    INDEX STOCK_DATE (STOCK_DATE ASC)
);

-- Windowed website-visit stats (sink for the streaming job)
CREATE DATABASE IF NOT EXISTS website_stats;
CREATE TABLE IF NOT EXISTS website_stats.visit_stats (
    ID                  INT         NOT NULL AUTO_INCREMENT,
    INTERVAL_TIMESTAMP  DATETIME    DEFAULT NULL,
    LAST_ACTION         VARCHAR(45) DEFAULT NULL,
    DURATION            INT         DEFAULT NULL,
    PRIMARY KEY (ID),
    INDEX INTERVAL_TIMESTAMP (INTERVAL_TIMESTAMP ASC)
);

GRANT ALL PRIVILEGES ON warehouse_stock.* TO 'spark'@'%';
GRANT ALL PRIVILEGES ON global_stock.*    TO 'spark'@'%';
GRANT ALL PRIVILEGES ON website_stats.*   TO 'spark'@'%';
FLUSH PRIVILEGES;
