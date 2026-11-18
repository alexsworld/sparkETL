package com.sparketl.connectors.jdbc

import java.sql.{Connection, DriverManager, ResultSet, Statement}
import java.util.UUID

import scala.util.control.NonFatal

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import com.sparketl.core.config.ConfigException
import com.sparketl.core.spi.{DataSourceProvider, Reader, Writer}
import com.sparketl.core.util.SensitiveDataMasker

/**
 * JDBC 关系型数据库连接器（providerType = "jdbc"）。
 *
 * 读取选项：
 *  - url（必填）、user、password、driver（可选）、table | query（二选一，query 优先，
 *    以子查询形式作为 dbtable）、fetchsize（可选）；
 *  - 并行读取：partitionColumn + lowerBound + upperBound + numPartitions，或
 *    predicates（分号分隔的自定义 WHERE 分片）；两者均未配置时打 WARN 提示；
 *  - partitionColumn 与 predicates 互斥，同时配置视为配置错误。
 *
 * 写入选项：
 *  - url、user、password、driver、table（必填）；
 *  - mode: append -> append；overwrite -> overwrite；
 *    upsert（或 append 且携带 upsertKeys）-> staging 表 + 方言合并 SQL 语义：
 *    先将数据写入目标库中的 staging 临时表，再按数据库方言生成 ON CONFLICT /
 *    ON DUPLICATE KEY / MERGE 语句完成幂等合并，最后清理 staging 表；
 *  - upsertKeys（逗号分隔）：合并主键列（仅写）。
 *
 * 日志中的 user / password 等敏感键均经 [[SensitiveDataMasker]] 脱敏。
 */
class JdbcProvider extends DataSourceProvider {

  private val log = LoggerFactory.getLogger(classOf[JdbcProvider])

  override def providerType: String = "jdbc"

  // ------------------------------------------------------------------
  //  Reader
  // ------------------------------------------------------------------

  override def createReader(options: Map[String, String]): Reader = new Reader {
    override def read(spark: SparkSession): DataFrame = {
      val url = requiredOption(options, "url")
      log.info(s"[jdbc] 读取: ${SensitiveDataMasker.maskAll(options)}")

      val dbtable = (options.get("query"), options.get("table")) match {
        case (Some(query), _) => s"($query) JDBC_SUBQUERY"
        case (None, Some(table)) => table
        case _ => throw new ConfigException("[jdbc] 读取需要配置 'table' 或 'query' 选项之一")
      }

      val hasPartitionColumn = options.contains("partitionColumn")
      val hasPredicates = options.contains("predicates")
      if (hasPartitionColumn && hasPredicates) {
        throw new ConfigException(
          "[jdbc] partitionColumn 与 predicates 互斥，只能二选一配置并行读取分片")
      }
      // 透传选项（含 user / password / driver / fetchsize 等，不含结构化分片参数）
      val passThrough = options - "table" - "query"

      if (hasPredicates) {
        // predicates 分片只能经 spark.read.jdbc(url, table, predicates, props) 重载传递，
        // 其余选项全部放入 Properties（user / password / driver / fetchsize 均会被识别）
        val preds = splitSemicolon(options("predicates")).toArray
        spark.read.jdbc(url, dbtable, preds, connectionProps(passThrough))
      } else {
        val reader = spark.read.format("jdbc")
          .option("url", url)
          .option("dbtable", dbtable)
          .options(passThrough)
        if (hasPartitionColumn) {
          reader.option("partitionColumn", options("partitionColumn").trim)
          options.get("lowerBound").foreach(v => reader.option("lowerBound", v.trim.toLong))
          options.get("upperBound").foreach(v => reader.option("upperBound", v.trim.toLong))
          options.get("numPartitions").foreach(v => reader.option("numPartitions", v.trim.toInt))
        } else {
          log.warn(s"[jdbc] 未配置 partitionColumn 或 predicates，将单分区串行读取，" +
            s"大数据量建议配置分片以并行读取: url=$url")
        }
        reader.load()
      }
    }
  }

  // ------------------------------------------------------------------
  //  Writer
  // ------------------------------------------------------------------

  override def createWriter(mode: String, options: Map[String, String]): Writer = new Writer {
    override def write(spark: SparkSession, df: DataFrame): Unit = {
      log.info(s"[jdbc] 写入: ${SensitiveDataMasker.maskAll(options)}, mode=$mode")
      mode match {
        case "upsert" => upsert(df, options)
        case "append" if options.contains("upsertKeys") => upsert(df, options)
        case "append" => simpleWrite(df, options, "append")
        case "overwrite" => simpleWrite(df, options, "overwrite")
        case other =>
          throw new ConfigException(
            s"[jdbc] 不支持的写入模式 '$other'（支持 append | overwrite | upsert）")
      }
    }
  }

  // ------------------------------------------------------------------
  //  私有辅助
  // ------------------------------------------------------------------

  private def requiredOption(options: Map[String, String], key: String): String =
    options.getOrElse(key, throw new ConfigException(s"[jdbc] 缺少必填选项 '$key'"))

  /** 将选项映射转为 java.util.Properties（spark.read.jdbc 的 predicates 重载需要）。 */
  private def connectionProps(params: Map[String, String]): java.util.Properties = {
    val props = new java.util.Properties()
    params.foreach { case (k, v) => props.setProperty(k, v) }
    props
  }

  /** 普通 append / overwrite 直写目标表。 */
  private def simpleWrite(df: DataFrame, options: Map[String, String], sparkMode: String): Unit = {
    val table = requiredOption(options, "table")
    val writer = baseJdbcWriter(df, options).option("dbtable", table).mode(sparkMode)
    writer.save()
    log.info(s"[jdbc] 写入完成: table=$table, mode=$sparkMode")
  }

  /**
   * upsert 语义：staging 表 + 方言合并 SQL。
   *
   * @see 模块 ScalaDoc 中三种方言的合并语句说明
   */
  private def upsert(df: DataFrame, options: Map[String, String]): Unit = {
    val url = requiredOption(options, "url")
    val table = requiredOption(options, "table")
    val dialect = dialectOf(url)
    val quote = quoteId(dialect)(_)
    val keys = splitCsv(requiredOption(options, "upsertKeys"))
    val staging = table + "_staging_" + UUID.randomUUID().toString.substring(0, 8)

    var conn: Connection = null
    try {
      conn = openConnection(options)
      exec(conn, s"DROP TABLE IF EXISTS ${quote(staging)}")

      // 1. 先写 staging 临时表
      baseJdbcWriter(df, options).option("dbtable", staging).mode("append").save()

      // 2. 读取 staging 列名，并校验 upsertKeys 均存在
      val columns = readColumns(conn, dialect, staging)
      val unknown = keys.filterNot(columns.contains)
      if (unknown.nonEmpty) {
        throw new ConfigException(s"[jdbc] upsertKeys (${unknown.mkString(", ")}) " +
          s"不在表 $table 的列中，可用列: ${columns.mkString(", ")}")
      }

      // 3. 按方言生成合并 SQL 并执行
      val mergeSql = buildMergeSql(dialect, table, staging, columns, keys)
      log.info(s"[jdbc] 执行 upsert 合并: table=$table, dialect=$dialect, " +
        s"keys=${keys.mkString(",")}, staging=$staging")
      exec(conn, mergeSql)
      log.info(s"[jdbc] upsert 完成: table=$table")
    } finally {
      try {
        if (conn != null) {
          try exec(conn, s"DROP TABLE IF EXISTS ${quote(staging)}")
          catch { case NonFatal(e) => log.warn(s"staging 表清理失败: $staging", e) }
          try conn.close()
          catch { case NonFatal(e) => log.warn("JDBC 连接关闭失败", e) }
        }
      }
    }
  }

  /** 组装带 url / user / password / driver / fetchsize 的 JDBC DataFrameWriter。 */
  private def baseJdbcWriter(df: DataFrame, options: Map[String, String]) = {
    val writer = df.write.format("jdbc")
      .option("url", options("url"))
      .option("user", options.getOrElse("user", ""))
      .option("password", options.getOrElse("password", ""))
    options.get("driver").foreach(writer.option("driver", _))
    options.get("fetchsize").foreach(v => writer.option("fetchsize", v.trim.toInt))
    writer
  }

  /** 建立元数据操作连接（driver 存在时先显式加载）。 */
  private def openConnection(options: Map[String, String]): Connection = {
    options.get("driver").foreach(Class.forName)
    val url = options("url")
    options.get("user") match {
      case Some(user) => DriverManager.getConnection(url, user, options.getOrElse("password", ""))
      case None => DriverManager.getConnection(url)
    }
  }

  /** SELECT * WHERE 1=0 读取表的列名（ResultSetMetaData）。 */
  private def readColumns(conn: Connection, dialect: String, table: String): Seq[String] = {
    var stmt: Statement = null
    var rs: ResultSet = null
    try {
      stmt = conn.createStatement()
      rs = stmt.executeQuery(s"SELECT * FROM ${quoteId(dialect)(table)} WHERE 1=0")
      val meta = rs.getMetaData
      (1 to meta.getColumnCount).map(meta.getColumnName)
    } finally {
      if (rs != null) try rs.close() catch { case NonFatal(_) => }
      if (stmt != null) try stmt.close() catch { case NonFatal(_) => }
    }
  }

  /**
   * 按方言生成合并 SQL。
   *  - postgres：INSERT ... ON CONFLICT (keys) DO UPDATE SET col = EXCLUDED.col
   *  - mysql   ：INSERT ... ON DUPLICATE KEY UPDATE col = VALUES(col)
   *  - h2      ：MERGE INTO ... USING ... ON keys ... WHEN MATCHED / NOT MATCHED
   */
  private def buildMergeSql(
      dialect: String,
      table: String,
      staging: String,
      columns: Seq[String],
      keys: Seq[String]): String = {

    val q = quoteId(dialect)(_)
    val allCols = columns.map(q).mkString(", ")
    val nonKeyCols = columns.filterNot(keys.contains)

    dialect match {
      case "postgres" =>
        val updateClause =
          if (nonKeyCols.isEmpty) "DO NOTHING"
          else "DO UPDATE SET " + nonKeyCols.map(c => s"${q(c)} = EXCLUDED.${q(c)}").mkString(", ")
        s"INSERT INTO ${q(table)} ($allCols) SELECT $allCols FROM ${q(staging)} " +
          s"ON CONFLICT (${keys.map(q).mkString(", ")}) $updateClause"

      case "mysql" =>
        val updateClause =
          if (nonKeyCols.isEmpty) s"${q(keys.head)} = VALUES(${q(keys.head)})"
          else nonKeyCols.map(c => s"${q(c)} = VALUES(${q(c)})").mkString(", ")
        s"INSERT INTO ${q(table)} ($allCols) SELECT $allCols FROM ${q(staging)} " +
          s"ON DUPLICATE KEY UPDATE $updateClause"

      case "h2" =>
        val on = keys.map(k => s"${q(table)}.${q(k)} = ${q(staging)}.${q(k)}").mkString(" AND ")
        val matchedClause =
          if (nonKeyCols.isEmpty) ""
          else " WHEN MATCHED THEN UPDATE SET " +
            nonKeyCols.map(c => s"${q(table)}.${q(c)} = ${q(staging)}.${q(c)}").mkString(", ")
        val insertCols = columns.map(c => s"${q(staging)}.${q(c)}").mkString(", ")
        s"MERGE INTO ${q(table)} USING ${q(staging)} ON $on" +
          s"$matchedClause WHEN NOT MATCHED THEN INSERT ($allCols) VALUES ($insertCols)"
    }
  }

  /** 由 url 前缀判断 upsert 方言，不支持的数据库快速失败。 */
  private def dialectOf(url: String): String =
    if (url.startsWith("jdbc:postgresql")) "postgres"
    else if (url.startsWith("jdbc:mysql") || url.startsWith("jdbc:mariadb")) "mysql"
    else if (url.startsWith("jdbc:h2")) "h2"
    else throw new ConfigException(
      s"[jdbc] 不支持的 upsert 数据库方言（url=$url），当前支持 postgresql / mysql / mariadb / h2")

  /** 标识符引用：mysql 用反引号，postgres / h2 用双引号。 */
  private def quoteId(dialect: String): String => String =
    if (dialect == "mysql") (id: String) => s"`$id`"
    else (id: String) => "\"" + id + "\""

  private def splitCsv(value: String): Seq[String] =
    value.split(",").map(_.trim).filter(_.nonEmpty).toSeq

  private def splitSemicolon(value: String): Seq[String] =
    value.split(";").map(_.trim).filter(_.nonEmpty).toSeq

  private def exec(conn: Connection, sql: String): Unit = {
    var stmt: Statement = null
    try {
      stmt = conn.createStatement()
      stmt.execute(sql)
    } finally {
      if (stmt != null) try stmt.close() catch { case NonFatal(_) => }
    }
  }
}
