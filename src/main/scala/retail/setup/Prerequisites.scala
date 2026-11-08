package retail.setup

/** The databases, tables and Kafka topics the pipelines expect.
  * Matches code_00_03_Spark_BDE_Setup_Prerequisites.ipynb and
  * infra/mysql/init/01-schema.sql.
  */
object Prerequisites:

  final case class Database(name: String, tableDdl: String)

  val databases: List[Database] = List(
    Database(
      "warehouse_stock",
      """CREATE TABLE IF NOT EXISTS `warehouse_stock`.`item_stock` (
        |  `ID`            INT           NOT NULL AUTO_INCREMENT,
        |  `STOCK_DATE`    DATETIME      NOT NULL,
        |  `WAREHOUSE_ID`  VARCHAR(45)   NOT NULL,
        |  `ITEM_NAME`     VARCHAR(45)   NOT NULL,
        |  `OPENING_STOCK` INT           NOT NULL DEFAULT 0,
        |  `RECEIPTS`      INT           NOT NULL DEFAULT 0,
        |  `ISSUES`        INT           NOT NULL DEFAULT 0,
        |  `UNIT_VALUE`    DECIMAL(10,2) NOT NULL DEFAULT 0,
        |  PRIMARY KEY (`ID`),
        |  INDEX `STOCK_DATE` (`STOCK_DATE` ASC))""".stripMargin
    ),
    Database(
      "global_stock",
      """CREATE TABLE IF NOT EXISTS `global_stock`.`item_stock` (
        |  `ID`            INT           NOT NULL AUTO_INCREMENT,
        |  `STOCK_DATE`    DATETIME      NOT NULL,
        |  `ITEM_NAME`     VARCHAR(45)   NOT NULL,
        |  `TOTAL_REC`     INT           NOT NULL DEFAULT 0,
        |  `OPENING_STOCK` INT           NOT NULL DEFAULT 0,
        |  `RECEIPTS`      INT           NOT NULL DEFAULT 0,
        |  `ISSUES`        INT           NOT NULL DEFAULT 0,
        |  `CLOSING_STOCK` INT           NOT NULL DEFAULT 0,
        |  `CLOSING_VALUE` DECIMAL(10,2) NOT NULL DEFAULT 0,
        |  PRIMARY KEY (`ID`),
        |  INDEX `STOCK_DATE` (`STOCK_DATE` ASC))""".stripMargin
    ),
    Database(
      "website_stats",
      """CREATE TABLE IF NOT EXISTS `website_stats`.`visit_stats` (
        |  `ID`                 INT         NOT NULL AUTO_INCREMENT,
        |  `INTERVAL_TIMESTAMP` DATETIME    DEFAULT NULL,
        |  `LAST_ACTION`        VARCHAR(45) DEFAULT NULL,
        |  `DURATION`           INT         DEFAULT NULL,
        |  PRIMARY KEY (`ID`),
        |  INDEX `INTERVAL_TIMESTAMP` (`INTERVAL_TIMESTAMP` ASC))""".stripMargin
    )
  )

  val topics: List[String] = List(
    "spark.streaming.website.visits",
    "spark.streaming.carts.abandoned",
    "spark.exercise.lastaction.long"
  )
