package retail.batch

import java.time.LocalDate
import scala.util.{Random, Using}

import retail.config.InfraConfig

/** Notebook section 03.03 "Setup the local database": random daily stock per warehouse.
  *
  * Changes from the notebook:
  *  - rows are inserted with a batched PreparedStatement (the notebook built SQL with f-strings);
  *  - instead of `DELETE FROM item_stock` (whole table), only the rows for the generated
  *    dates and warehouses are replaced, so re-running gives the same row counts.
  */
object StockGenerator:

  final case class StockRow(
      stockDate: LocalDate,
      warehouseId: String,
      itemName: String,
      openingStock: Int,
      receipts: Int,
      issues: Int,
      unitValue: BigDecimal
  )

  /** Products and their unit values, as in the notebook. */
  val items: List[(String, BigDecimal)] = List(
    "Tape Dispenser"   -> BigDecimal("5.99"),
    "Pencil Sharpener" -> BigDecimal("10.00"),
    "Labeling Machine" -> BigDecimal("25.00"),
    "Calculator"       -> BigDecimal("14.99"),
    "Scissors"         -> BigDecimal("7.99"),
    "Sticky Notes"     -> BigDecimal("2.00"),
    "Notebook"         -> BigDecimal("2.50"),
    "Clipboard"        -> BigDecimal("12.00"),
    "Folder"           -> BigDecimal("1.00"),
    "Pencil Box"       -> BigDecimal("2.99")
  )

  /** One row per date and item. Ranges match the notebook's `random.randint` (inclusive):
    * opening 1..100, receipts 1..50, issues 1..(opening + receipts).
    */
  def generateRows(dates: Seq[LocalDate], warehouseId: String, rng: Random): Seq[StockRow] =
    for
      date <- dates
      (item, unitValue) <- items
    yield
      val opening = rng.between(1, 101)
      val receipts = rng.between(1, 51)
      val issues = rng.between(1, opening + receipts + 1)
      StockRow(date, warehouseId, item, opening, receipts, issues, unitValue)

  /** Replaces the rows for these dates and warehouses. Returns the number of rows inserted. */
  def replaceStock(cfg: InfraConfig, rows: Seq[StockRow]): Int =
    Using.resource(cfg.connect("warehouse_stock")) { conn =>
      conn.setAutoCommit(false)
      try
        Using.resource(conn.prepareStatement(
          "DELETE FROM item_stock WHERE DATE(STOCK_DATE) = ? AND WAREHOUSE_ID = ?"
        )) { del =>
          rows.map(r => (r.stockDate, r.warehouseId)).distinct.foreach { (d, w) =>
            del.setObject(1, d)
            del.setString(2, w)
            del.addBatch()
          }
          del.executeBatch()
        }
        val inserted = Using.resource(conn.prepareStatement(
          """INSERT INTO item_stock
            |  (STOCK_DATE, WAREHOUSE_ID, ITEM_NAME, OPENING_STOCK, RECEIPTS, ISSUES, UNIT_VALUE)
            |VALUES (?, ?, ?, ?, ?, ?, ?)""".stripMargin
        )) { ins =>
          rows.foreach { r =>
            ins.setObject(1, r.stockDate.atStartOfDay())
            ins.setString(2, r.warehouseId)
            ins.setString(3, r.itemName)
            ins.setInt(4, r.openingStock)
            ins.setInt(5, r.receipts)
            ins.setInt(6, r.issues)
            ins.setBigDecimal(7, r.unitValue.bigDecimal)
            ins.addBatch()
          }
          ins.executeBatch().length
        }
        conn.commit()
        inserted
      catch
        case e: Throwable =>
          conn.rollback()
          throw e
    }

  /** Row counts per warehouse, like the notebook's final check. */
  def countsByWarehouse(cfg: InfraConfig): List[(String, Int)] =
    Using.resource(cfg.connect("warehouse_stock")) { conn =>
      Using.resource(conn.createStatement()) { st =>
        Using.resource(st.executeQuery(
          "SELECT WAREHOUSE_ID, COUNT(*) FROM item_stock GROUP BY WAREHOUSE_ID ORDER BY WAREHOUSE_ID"
        )) { rs =>
          Iterator.continually(rs).takeWhile(_.next()).map(r => r.getString(1) -> r.getInt(2)).toList
        }
      }
    }

  def run(cfg: InfraConfig, dates: Seq[LocalDate], warehouses: Seq[String], rng: Random = Random()): Unit =
    warehouses.foreach { w =>
      println(s"Generating sample data for warehouse : $w (${dates.mkString(", ")})")
      replaceStock(cfg, generateRows(dates, w, rng))
    }
    println("\nRecords in warehouse_stock:\n---------------------------")
    countsByWarehouse(cfg).foreach((w, n) => println(s"$w : $n"))
