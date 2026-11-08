import java.net.{InetSocketAddress, Socket}
import java.nio.file.Files
import java.time.LocalDate
import scala.util.{Random, Try, Using}

import org.apache.spark.sql.SparkSession

import retail.batch.{GlobalAggregator, StockGenerator, StockPipeline, StockUploader}
import retail.config.InfraConfig

/** End-to-end run of the notebook flow against the docker-compose stack. Skipped when MySQL
  * isn't reachable. It replaces warehouse_stock/global_stock rows for the three dates below,
  * which are kept in the past so it doesn't touch the data a manual run creates for "today".
  */
class StockPipelineIntegrationSuite extends munit.FunSuite:

  private val baseCfg = InfraConfig.fromEnv()
  private var spark: SparkSession = scala.compiletime.uninitialized

  override def beforeAll(): Unit =
    spark = retail.config.Spark.localSession("StockPipelineIntegrationSuite")

  override def afterAll(): Unit =
    if spark != null then spark.stop()

  private def mysqlUp: Boolean =
    Try(Using.resource(Socket())(_.connect(InetSocketAddress(baseCfg.mysqlHost, baseCfg.mysqlPort), 1000))).isSuccess

  private val dates = StockPipeline.defaultDates(LocalDate.parse("2001-01-03"))

  private def runOnce(cfg: InfraConfig): Unit =
    StockPipeline.warehouses.foreach(w => StockGenerator.replaceStock(cfg, StockGenerator.generateRows(dates, w, Random(1))))
    StockPipeline.warehouses.foreach(w => StockUploader.upload(spark, cfg, dates.min, dates.max, w))
    GlobalAggregator.run(spark, cfg)

  private def globalRowCount(cfg: InfraConfig): Int =
    Using.resource(cfg.connect("global_stock")) { conn =>
      Using.resource(conn.prepareStatement(
        "SELECT COUNT(*) FROM item_stock WHERE STOCK_DATE BETWEEN ? AND ?"
      )) { st =>
        st.setObject(1, dates.min.atStartOfDay())
        st.setObject(2, dates.max.atStartOfDay())
        Using.resource(st.executeQuery())(rs => { rs.next(); rs.getInt(1) })
      }
    }

  test("generate -> upload -> aggregate: global totals reconcile with the warehouses, and re-runs are idempotent") {
    assume(mysqlUp, "MySQL not reachable; start the stack with `docker compose up -d --wait`")
    val cfg = baseCfg.copy(rawDataDir = Files.createTempDirectory("raw_data").toString)

    runOnce(cfg)
    dates.foreach(d => assertEquals(GlobalAggregator.reconcile(cfg, d), Map.empty, s"mismatch on $d"))
    val expectedRows = dates.size * StockGenerator.items.size
    assertEquals(globalRowCount(cfg), expectedRows)

    runOnce(cfg) // the notebook's append mode would double the totals here
    dates.foreach(d => assertEquals(GlobalAggregator.reconcile(cfg, d), Map.empty, s"mismatch on $d after re-run"))
    assertEquals(globalRowCount(cfg), expectedRows)
    assertEquals(spark.read.parquet(cfg.rawDataDir).count(), (expectedRows * StockPipeline.warehouses.size).toLong)
  }

  test("upload skips a warehouse with no rows") {
    assume(mysqlUp, "MySQL not reachable")
    val cfg = baseCfg.copy(rawDataDir = Files.createTempDirectory("raw_data").toString)
    assertEquals(StockUploader.upload(spark, cfg, dates.min, dates.max, "Atlantis"), 0L)
  }
