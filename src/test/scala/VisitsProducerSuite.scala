import java.net.{InetSocketAddress, Socket}
import java.time.{Clock, Instant, LocalDateTime, ZoneOffset}
import scala.util.{Random, Try, Using}

import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.types.*

import retail.config.InfraConfig
import retail.streaming.VisitsProducer
import retail.streaming.VisitsProducer.Visit

class VisitsProducerSuite extends munit.FunSuite:

  private val cfg = InfraConfig.fromEnv()
  private var spark: SparkSession = scala.compiletime.uninitialized

  override def beforeAll(): Unit = spark = retail.config.Spark.localSession("VisitsProducerSuite")
  override def afterAll(): Unit = if spark != null then spark.stop()

  private val fixedClock = Clock.fixed(Instant.parse("2026-11-08T10:15:30.123Z"), ZoneOffset.UTC)

  test("random visits stay within the notebook's value lists and ranges") {
    val rng = Random(3)
    val visits = Seq.fill(500)(VisitsProducer.randomVisit(rng, fixedClock))
    visits.foreach { v =>
      assert(VisitsProducer.countries.contains(v.country), v)
      assert(VisitsProducer.lastActions.contains(v.lastAction), v)
      assert(v.duration >= 1 && v.duration <= 20, v)
      assertEquals(v.visitDate, LocalDateTime.parse("2026-11-08T10:15:30")) // seconds precision
      assertEquals(v.key, v.country)
    }
    // with 500 draws every value should show up
    assertEquals(visits.map(_.country).toSet, VisitsProducer.countries.toSet)
    assertEquals(visits.map(_.lastAction).toSet, VisitsProducer.lastActions.toSet)
    assertEquals(visits.map(_.duration).toSet, (1 to 20).toSet)
  }

  test("JSON matches the notebook's json.dumps output") {
    val v = Visit("Brazil", "ShoppingCart", LocalDateTime.parse("2024-08-12T13:01:28"), 9)
    assertEquals(
      v.toJson,
      """{"country": "Brazil", "last_action": "ShoppingCart", "visit_date": "2024-08-12 13:01:28", "duration": 9}"""
    )
  }

  test("JSON parses with the schema the streaming job (code_04_04) uses") {
    val schema = StructType(Seq(
      StructField("country", StringType),
      StructField("last_action", StringType),
      StructField("visit_date", TimestampNTZType),
      StructField("duration", IntegerType)
    ))
    val v = Visit("India", "FAQ", LocalDateTime.parse("2026-11-08T10:15:30"), 15)
    val row = spark.createDataset(Seq(v.toJson))(using Encoders.STRING)
      .select(from_json(col("value"), schema).as("v"))
      .select("v.*")
      .head()
    assertEquals(row.getString(0), "India")
    assertEquals(row.getString(1), "FAQ")
    assertEquals(row.getAs[LocalDateTime]("visit_date"), v.visitDate)
    assertEquals(row.getInt(3), 15)
  }

  private def kafkaUp: Boolean =
    val Array(h, p) = cfg.kafkaBootstrap.split(",").head.split(":")
    Try(Using.resource(Socket())(_.connect(InetSocketAddress(h, p.toInt), 1000))).isSuccess

  test("produced visits arrive on the topic, keyed by country") {
    assume(kafkaUp, "Kafka not reachable; start the stack with `docker compose up -d --wait`")
    val sent = VisitsProducer.produce(cfg, count = 5, maxDelaySeconds = 0, rng = Random(11), log = _ => ())
    val got = VisitsProducer.peekLatest(cfg, VisitsProducer.topic, 5)
    assertEquals(got.map(r => r.key -> r.value).toList, sent.map(v => v.key -> v.toJson).toList)
  }
