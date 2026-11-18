package com.sparketl.connectors.hive

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import com.sparketl.core.config.ConfigException
import com.sparketl.core.spi.{DataSourceProvider, Reader, Writer}
import com.sparketl.core.util.SensitiveDataMasker

/**
 * Hive 连接器（providerType = "hive"）。
 *
 * 读取选项：
 *  - query（优先）或 table（二选一必填），query 直接走 spark.sql；
 *  - 其余选项透传给 spark.read（对 Hive 表主要供诊断与扩展使用）。
 *
 * 写入选项：
 *  - table（必填）、partitionBy（逗号分隔，可选）；
 *  - mode: append -> append、overwrite -> overwrite；partitionBy 非空时按列分区。
 *
 * 前置条件：SparkSession 已启用 Hive 支持（enableHiveSupport，由
 * sparketl-core 的 SparkSessionFactory 统一开启）；overwrite 写入分区表时的
 * 动态分区覆盖由 spark.sql.sources.partitionOverwriteMode=dynamic 保证。
 */
class HiveProvider extends DataSourceProvider {

  private val log = LoggerFactory.getLogger(classOf[HiveProvider])

  override def providerType: String = "hive"

  override def createReader(options: Map[String, String]): Reader = new Reader {
    override def read(spark: SparkSession): DataFrame = {
      (options.get("query"), options.get("table")) match {
        case (Some(query), _) =>
          log.info(s"[hive] 执行 SQL 查询: $query")
          spark.sql(query)
        case (None, Some(table)) =>
          val others = options - "query" - "table"
          log.info(s"[hive] 读取 Hive 表: table=$table, 透传选项=${SensitiveDataMasker.maskAll(others)}")
          spark.read.options(others).table(table)
        case _ =>
          throw new ConfigException("[hive] 读取需要配置 'query' 或 'table' 选项之一")
      }
    }
  }

  override def createWriter(mode: String, options: Map[String, String]): Writer = new Writer {
    override def write(spark: SparkSession, df: DataFrame): Unit = {
      val table = options.getOrElse("table",
        throw new ConfigException("[hive] 写入缺少必填选项 'table'"))
      val partitionCols = splitCsv(options.getOrElse("partitionBy", ""))
      val sparkMode = mode match {
        case "append"    => "append"
        case "overwrite" => "overwrite"
        case other =>
          throw new ConfigException(s"[hive] 不支持的写入模式 '$other'（仅支持 append | overwrite）")
      }
      val base = df.write.mode(sparkMode)
      val writer = if (partitionCols.nonEmpty) base.partitionBy(partitionCols: _*) else base
      log.info(s"[hive] 写入表: table=$table, mode=$sparkMode, " +
        s"partitionBy=${if (partitionCols.isEmpty) "-" else partitionCols.mkString(",")}")
      writer.saveAsTable(table)
    }
  }

  /** 按逗号拆分为去空白的非空列表。 */
  private def splitCsv(value: String): Seq[String] =
    Option(value).map(_.split(",").map(_.trim).filter(_.nonEmpty).toSeq).getOrElse(Seq.empty)
}
