package retail.setup

import java.nio.file.{Files, Path, Paths}
import java.sql.{Connection, DriverManager}
import java.util.Properties
import scala.jdk.CollectionConverters.*
import scala.util.Using

import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, NewTopic}
import retail.config.{InfraConfig, Spark}

@main def runSetup(): Unit =
  val cfg  = InfraConfig.fromEnv()
  SetupPrerequisites.checkJava()
  SetupPrerequisites.setupMySql(cfg)
  SetupPrerequisites.setupKafkaTopics(cfg)
  SetupPrerequisites.createRawDataDir(cfg)
  SetupPrerequisites.verifyWithSpark(cfg)
  println("\nAll prerequisites are in place.")

object SetupPrerequisites:

  private def header(title: String): Unit =
    println(s"\n### $title\n" + "-" * 40)

  // ---------------------------------------------------------------- Java
  def checkJava(): Unit =
    header("Java")
    val version = Runtime.version().feature()
    println(s"Java version : ${System.getProperty("java.version")}")
    println(s"JAVA_HOME    : ${sys.env.getOrElse("JAVA_HOME", "<not set>")}")
    require(version >= 17, s"Spark 4 needs Java 17 or newer, found Java $version")

  // ---------------------------------------------------------------- MySQL
  /** Returns the databases that were newly created. */
  def setupMySql(cfg: InfraConfig): List[String] =
    header("Setup MySQL")
    Using.resource(DriverManager.getConnection(cfg.jdbcUrl("mysql"), "root", cfg.mysqlRootPassword)) { conn =>
      val existing = listDatabases(conn)
      println(s"Databases available : ${existing.mkString(", ")}")

      val created = Prerequisites.databases.flatMap { db =>
        if existing.contains(db.name) then
          println(s"${db.name} DB already exists.")
          None
        else
          println(s"Going to create ${db.name} DB")
          Some(db.name)
      }

      // Statements use IF NOT EXISTS, so tables and grants are also repaired
      // for databases that exist but are incomplete.
      Using.resource(conn.createStatement()) { st =>
        Prerequisites.databases.foreach { db =>
          st.execute(s"CREATE DATABASE IF NOT EXISTS `${db.name}`")
          st.execute(db.tableDdl)
          st.execute(s"GRANT ALL PRIVILEGES ON `${db.name}`.* TO '${cfg.mysqlUser}'@'%'")
        }
        st.execute("FLUSH PRIVILEGES")
      }
      created.foreach(db => println(s"$db DB successfully created"))
      created
    }

  private def listDatabases(conn: Connection): Set[String] =
    Using.resource(conn.createStatement()) { st =>
      Using.resource(st.executeQuery("SHOW DATABASES")) { rs =>
        Iterator.continually(rs).takeWhile(_.next()).map(_.getString(1)).toSet
      }
    }

  // ---------------------------------------------------------------- Kafka / Redpanda
  /** Returns the topics that were newly created. */
  def setupKafkaTopics(cfg: InfraConfig): List[String] =
    header("Setup Kafka topics")
    val props = Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.kafkaBootstrap)
    props.put(AdminClientConfig.CLIENT_ID_CONFIG, "spark-de")
    props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000")
    props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "15000")

    Using.resource(Admin.create(props)) { admin =>
      val existing = admin.listTopics().names().get().asScala.toSet
      val missing = Prerequisites.topics.filterNot(existing.contains)

      if missing.isEmpty then println("All topics already exist.")
      else
        println(s"Creating topics : ${missing.mkString(", ")}")
        val newTopics = missing.map(t => NewTopic(t, 1, 1.toShort)).asJava
        admin.createTopics(newTopics).all().get()

      println("Topics available now:")
      admin.listTopics().names().get().asScala.toList.sorted.foreach(t => println(s"  $t"))
      missing
    }

  // ---------------------------------------------------------------- raw data folder
  /** Local folder standing in for the distributed file system (S3/HDFS). */
  def createRawDataDir(cfg: InfraConfig): Path =
    header("Raw data folder")
    val dir = Files.createDirectories(Paths.get(cfg.rawDataDir)).toAbsolutePath
    println(s"Raw data folder : $dir")
    dir

  // ---------------------------------------------------------------- verification
  /** Reads the table list through Spark's JDBC source as the `spark` user. This also
    * checks the grants, since the user only sees tables it has privileges on.
    */
  def verifyWithSpark(cfg: InfraConfig): Set[(String, String)] =
    header("Schema and tables (read with Spark)")
    Spark.withSession("SetupPrerequisitesJob") { spark =>
      val schemas = Prerequisites.databases.map(d => s"'${d.name}'").mkString(",")
      val df = spark.read
        .format("jdbc")
        .options(cfg.jdbcOptions("information_schema"))
        .option(
          "query",
          s"""SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.tables
             |WHERE TABLE_SCHEMA IN ($schemas)""".stripMargin
        )
        .load()
      df.orderBy("TABLE_SCHEMA").show(truncate = false)

      val found = df.collect().map(r => r.getString(0) -> r.getString(1)).toSet
      val expected = Prerequisites.databases.map(_.name).toSet
      val missing = expected -- found.map(_._1)
      require(missing.isEmpty, s"User ${cfg.mysqlUser} cannot see databases: ${missing.mkString(", ")}")
      found
    }
