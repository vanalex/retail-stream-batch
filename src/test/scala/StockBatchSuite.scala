import java.nio.file.Files
import java.time.LocalDate
import scala.util.Random

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.*

import retail.batch.{GlobalAggregator, StockGenerator, StockUploader}

/** Unit tests for the stock batch jobs. No Docker needed. */
class StockBatchSuite extends munit.FunSuite:

  private var spark: SparkSession = scala.compiletime.uninitialized

  override def beforeAll(): Unit =
    spark = retail.config.Spark.localSession("StockBatchSuite")

  override def afterAll(): Unit =
    if spark != null then spark.stop()

  private val d1 = LocalDate.parse("2026-11-05")
  private val d2 = LocalDate.parse("2026-11-06")

  // Same columns and types as the JDBC read in StockUploader.readStock
  private val rawSchema = StructType(Seq(
    StructField("ID", IntegerType),
    StructField("STOCK_DATE", StringType),
    StructField("WAREHOUSE_ID", StringType),
    StructField("ITEM_NAME", StringType),
    StructField("OPENING_STOCK", IntegerType),
    StructField("RECEIPTS", IntegerType),
    StructField("ISSUES", IntegerType),
    StructField("UNIT_VALUE", DecimalType(10, 2))
  ))

  private def raw(rows: Row*) = spark.createDataFrame(java.util.List.of(rows*), rawSchema)
  private def dec(s: String) = java.math.BigDecimal(s)

  test("generator: one row per date x item, values within the notebook's ranges") {
    val rows = StockGenerator.generateRows(Seq(d1, d2), "London", Random(42))
    assertEquals(rows.size, 2 * StockGenerator.items.size)
    assertEquals(rows.map(r => (r.stockDate, r.itemName)).distinct.size, rows.size)
    rows.foreach { r =>
      assertEquals(r.warehouseId, "London")
      assert((1 to 100).contains(r.openingStock), r)
      assert((1 to 50).contains(r.receipts), r)
      assert(r.issues >= 1 && r.issues <= r.openingStock + r.receipts, r)
    }
  }

  test("generator: deterministic for a fixed seed") {
    assertEquals(
      StockGenerator.generateRows(Seq(d1), "NewYork", Random(7)),
      StockGenerator.generateRows(Seq(d1), "NewYork", Random(7))
    )
  }

  test("summary: sums across warehouses per date and item") {
    val df = raw(
      Row(1, "2026-11-05", "NewYork", "Notebook", 10, 5, 3, dec("2.50")),
      Row(2, "2026-11-05", "London",  "Notebook", 20, 0, 5, dec("2.50")),
      Row(3, "2026-11-05", "London",  "Folder",    7, 3, 1, dec("1.00")),
      Row(4, "2026-11-06", "London",  "Folder",    1, 1, 1, dec("1.00"))
    )
    val out = GlobalAggregator.summarize(spark, df)
      .orderBy("STOCK_DATE", "ITEM_NAME")
      .collect()
      .map(r => (r.getAs[String]("STOCK_DATE"), r.getAs[String]("ITEM_NAME"), r.getAs[Long]("TOTAL_REC"),
        r.getAs[Long]("OPENING_STOCK"), r.getAs[Long]("RECEIPTS"), r.getAs[Long]("ISSUES"),
        r.getAs[Long]("CLOSING_STOCK"), r.getAs[java.math.BigDecimal]("CLOSING_VALUE").doubleValue))
      .toList
    assertEquals(out, List(
      ("2026-11-05", "Folder",   1L,  7L, 3L, 1L,  9L,  9.0),
      ("2026-11-05", "Notebook", 2L, 30L, 5L, 8L, 27L, 67.5),
      ("2026-11-06", "Folder",   1L,  1L, 1L, 1L,  1L,  1.0)
    ))
  }

  test("central store: re-uploading a warehouse replaces its partitions instead of duplicating") {
    val dir = Files.createTempDirectory("raw_data").toString
    val london = raw(Row(1, "2026-11-05", "London", "Folder", 7, 3, 1, dec("1.00")))
    val newYork = raw(Row(2, "2026-11-05", "NewYork", "Folder", 2, 2, 2, dec("1.00")))

    StockUploader.writeRaw(london, dir)
    StockUploader.writeRaw(newYork, dir)
    StockUploader.writeRaw(london, dir) // re-run for London only

    val back = spark.read.parquet(dir)
    assertEquals(back.count(), 2L)
    assertEquals(
      back.select("WAREHOUSE_ID").collect().map(_.getString(0)).sorted.toList,
      List("London", "NewYork")
    )
    // Partition layout used by the course: STOCK_DATE=.../WAREHOUSE_ID=...
    assert(Files.isDirectory(java.nio.file.Paths.get(dir, "STOCK_DATE=2026-11-05", "WAREHOUSE_ID=London")))
  }
