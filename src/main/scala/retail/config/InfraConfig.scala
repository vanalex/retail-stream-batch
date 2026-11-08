package retail.config

/** Connection settings for the local stack in docker-compose.yml.
  * Each value can be overridden with an environment variable.
  */
final case class InfraConfig(
    mysqlHost: String,
    mysqlPort: Int,
    mysqlRootPassword: String,
    mysqlUser: String,
    mysqlPassword: String,
    kafkaBootstrap: String,
    redisHost: String,
    redisPort: Int,
    rawDataDir: String
):
  /** JDBC URL for a database. `allowPublicKeyRetrieval` is needed for MySQL 8's
    * caching_sha2_password login when the connection is not using TLS.
    */
  def jdbcUrl(database: String): String =
    s"jdbc:mysql://$mysqlHost:$mysqlPort/$database?allowPublicKeyRetrieval=true"

  /** Spark JDBC options for a database, logged in as the application user. */
  def jdbcOptions(database: String): Map[String, String] = Map(
    "driver" -> "com.mysql.cj.jdbc.Driver",
    "url" -> jdbcUrl(database),
    "user" -> mysqlUser,
    "password" -> mysqlPassword
  )

  /** Plain JDBC connection as the application user. The caller closes it. */
  def connect(database: String): java.sql.Connection =
    java.sql.DriverManager.getConnection(jdbcUrl(database), mysqlUser, mysqlPassword)

object InfraConfig:
  private def env(name: String, default: String): String =
    sys.env.get(name).filter(_.nonEmpty).getOrElse(default)

  def fromEnv(): InfraConfig = InfraConfig(
    mysqlHost = env("MYSQL_HOST", "localhost"),
    mysqlPort = env("MYSQL_PORT", "3306").toInt,
    mysqlRootPassword = env("MYSQL_ROOT_PASSWORD", "spark"),
    mysqlUser = env("MYSQL_USER", "spark"),
    mysqlPassword = env("MYSQL_PASSWORD", "spark"),
    kafkaBootstrap = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    redisHost = env("REDIS_HOST", "localhost"),
    redisPort = env("REDIS_PORT", "6379").toInt,
    rawDataDir = env("RAW_DATA_DIR", "raw_data")
  )
