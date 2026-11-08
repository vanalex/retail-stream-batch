package retail.streaming

import java.time.{Clock, Duration, LocalDateTime}
import java.time.format.DateTimeFormatter
import java.util.Properties
import scala.jdk.CollectionConverters.*
import scala.util.{Random, Using}

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import retail.config.InfraConfig

object VisitsProducer:

  val topic = "spark.streaming.website.visits"
  val countries: Vector[String] = Vector("USA", "India", "Brazil", "Australia", "Russia")
  val lastActions: Vector[String] = Vector("Catalog", "FAQ", "Order", "ShoppingCart")

  private val visitDateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  final case class Visit(country: String, lastAction: String, visitDate: LocalDateTime, duration: Int):
    /** Kafka key: one country always goes to the same partition, so its updates stay in order. */
    def key: String = country

    /** Same layout as Python's `json.dumps` in the notebook. Values come from the fixed lists
      * above, so no JSON escaping is needed.
      */
    def toJson: String =
      s"""{"country": "$country", "last_action": "$lastAction", "visit_date": "${visitDate.format(visitDateFormat)}", "duration": $duration}"""

  /** Random visit. Ranges match the notebook's `random.randint` (inclusive): duration 1..20. */
  def randomVisit(rng: Random, clock: Clock = Clock.systemDefaultZone()): Visit =
    Visit(
      country = countries(rng.nextInt(countries.size)),
      lastAction = lastActions(rng.nextInt(lastActions.size)),
      visitDate = LocalDateTime.now(clock).withNano(0),
      duration = rng.between(1, 21)
    )

  private def producerProps(cfg: InfraConfig): Properties =
    val p = Properties()
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.kafkaBootstrap)
    p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    p.put(ProducerConfig.ACKS_CONFIG, "all")
    p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "30000")
    p.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000")
    p.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "15000")
    p

  /** Sends `count` random visits, sleeping 1..maxDelaySeconds between them (notebook: 1..3).
    * A `maxDelaySeconds` of 0 sends without pausing. Returns the visits sent, in order.
    */
  def produce(
      cfg: InfraConfig,
      count: Int,
      maxDelaySeconds: Int,
      rng: Random = Random(),
      log: String => Unit = println
  ): Seq[Visit] =
    require(count >= 0, "count must be >= 0")
    require(maxDelaySeconds >= 0, "maxDelaySeconds must be >= 0")
    Using.resource(KafkaProducer[String, String](producerProps(cfg))) { producer =>
      val sent = (1 to count).map { i =>
        val visit = randomVisit(rng)
        log(visit.toJson)
        val ack = producer.send(ProducerRecord(topic, visit.key, visit.toJson))
        if maxDelaySeconds > 0 && i < count then Thread.sleep(rng.between(1, maxDelaySeconds + 1) * 1000L)
        visit -> ack
      }
      producer.flush()
      val acks: Seq[RecordMetadata] = sent.map(_._2.get()) // throws if the broker rejected a record
      log(s"Sent ${acks.size} visits to $topic")
      sent.map(_._1)
    }

  /** The last `n` messages of every partition of `topic`, oldest first. Doesn't join a consumer
    * group or commit offsets, so it never affects other consumers.
    */
  def peekLatest(cfg: InfraConfig, topic: String, n: Int): Seq[ConsumerRecord[String, String]] =
    val p = Properties()
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.kafkaBootstrap)
    p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    Using.resource(KafkaConsumer[String, String](p)) { consumer =>
      val partitions = consumer.partitionsFor(topic, Duration.ofSeconds(10)).asScala
        .map(pi => TopicPartition(pi.topic, pi.partition)).toList
      consumer.assign(partitions.asJava)
      val ends = consumer.endOffsets(partitions.asJava).asScala.view.mapValues(_.longValue).toMap
      val begins = consumer.beginningOffsets(partitions.asJava).asScala.view.mapValues(_.longValue).toMap
      partitions.foreach(tp => consumer.seek(tp, math.max(begins(tp), ends(tp) - n)))

      val wanted = partitions.map(tp => ends(tp) - math.max(begins(tp), ends(tp) - n)).sum
      val out = collection.mutable.ArrayBuffer.empty[ConsumerRecord[String, String]]
      val deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos
      while out.size < wanted && System.nanoTime() < deadline do
        out ++= consumer.poll(Duration.ofMillis(500)).asScala.filter(r => r.offset < ends(TopicPartition(r.topic, r.partition)))
      out.sortBy(r => (r.partition, r.offset)).toSeq
    }

@main def runVisitsProducer(args: String*): Unit =
  val count = args.headOption.map(_.toInt).getOrElse(100)
  val maxDelay = args.lift(1).map(_.toInt).getOrElse(3)
  VisitsProducer.produce(InfraConfig.fromEnv(), count, maxDelay)

@main def runVisitsPeek(args: String*): Unit =
  val n = args.headOption.map(_.toInt).getOrElse(10)
  val records = VisitsProducer.peekLatest(InfraConfig.fromEnv(), VisitsProducer.topic, n)
  println(s"Total messages : ${records.size}")
  records.foreach(r => println(s"${r.partition}:${r.offset}: key=${r.key} vs=${r.value}"))
