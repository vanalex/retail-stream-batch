package retail.batch

import java.time.LocalDate

import retail.config.{InfraConfig, Spark}

object StockPipeline:

  val warehouses: List[String] = List("NewYork", "LosAngeles", "London")

  /** The last three days including today, as in the notebook. */
  def defaultDates(today: LocalDate = LocalDate.now()): List[LocalDate] =
    List(today.minusDays(2), today.minusDays(1), today)

  def upload(cfg: InfraConfig, dates: Seq[LocalDate]): Long =
    Spark.withSession("StockUploaderJob") { spark =>
      warehouses.map(w => StockUploader.upload(spark, cfg, dates.min, dates.max, w)).sum
    }

  def aggregate(cfg: InfraConfig): Seq[LocalDate] =
    Spark.withSession("GlobalAggregatorJob")(spark => GlobalAggregator.run(spark, cfg))

  /** Prints the notebook's Pencil Box comparison and checks every item. Returns true when consistent. */
  def check(cfg: InfraConfig, dates: Seq[LocalDate]): Boolean =
    val (local, global) = GlobalAggregator.totals(cfg, dates.min)
    println(s"\nPencil Box on ${dates.min}: local (sum of warehouses) = ${local.get("Pencil Box")}")
    println(s"Pencil Box on ${dates.min}: global                    = ${global.get("Pencil Box")}")

    val mismatches = dates.map(d => d -> GlobalAggregator.reconcile(cfg, d)).filter(_._2.nonEmpty)
    if mismatches.isEmpty then println(s"Reconciliation OK for ${dates.mkString(", ")}")
    else mismatches.foreach((d, m) => println(s"MISMATCH on $d: $m"))
    mismatches.isEmpty

@main def runStockGenerator(): Unit =
  StockGenerator.run(InfraConfig.fromEnv(), StockPipeline.defaultDates(), StockPipeline.warehouses)

@main def runStockUploader(): Unit =
  val rows = StockPipeline.upload(InfraConfig.fromEnv(), StockPipeline.defaultDates())
  println(s"Uploaded $rows rows")

@main def runGlobalAggregator(): Unit =
  val cfg = InfraConfig.fromEnv()
  val dates = StockPipeline.aggregate(cfg)
  StockPipeline.check(cfg, dates)

@main def runStockPipeline(): Unit =
  val cfg = InfraConfig.fromEnv()
  val dates = StockPipeline.defaultDates()
  StockGenerator.run(cfg, dates, StockPipeline.warehouses)
  println(s"\nUploaded ${StockPipeline.upload(cfg, dates)} rows to ${cfg.rawDataDir}\n")
  StockPipeline.aggregate(cfg)
  if !StockPipeline.check(cfg, dates) then sys.exit(1)
