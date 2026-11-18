package com.sparketl.connectors.hdfs

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import com.sparketl.connectors.util.FileStoreLogic
import com.sparketl.core.config.ConfigException
import com.sparketl.core.spi.{DataSourceProvider, Reader, Writer}
import com.sparketl.core.util.SensitiveDataMasker

/**
 * HDFS / 本地文件系统连接器（providerType = "hdfs"）。
 *
 * 读取选项：
 *  - path（必填）：文件或目录路径；
 *  - format（可选，默认 parquet）：parquet | orc | csv | json | text；
 *  - 其余选项透传给 spark.read（如 csv 的 header / delimiter）。
 *
 * 写入选项：
 *  - path（必填）、format（默认 parquet）、partitionBy（逗号分隔，可选）；
 *  - mode: append | overwrite，统一走 [[FileStoreLogic]] 的 staging + 原子提交，
 *    保证重跑幂等、失败不留脏数据。
 */
class HdfsProvider extends DataSourceProvider {

  private val log = LoggerFactory.getLogger(classOf[HdfsProvider])

  /** 支持的文件格式（与扩展名映射一起维护）。 */
  private val SupportedFormats = Set("parquet", "orc", "csv", "json", "text")

  /** 文件格式 -> 数据文件扩展名（用于日志与 append 合并语义标注）。 */
  private val FormatExtensions = Map(
    "parquet" -> ".parquet", "orc" -> ".orc",
    "csv" -> ".csv", "json" -> ".json", "text" -> ".txt")

  override def providerType: String = "hdfs"

  /** 子类（如 S3）在真正读写前注入文件系统级配置的钩子，默认空实现。 */
  protected def configureFileSystem(spark: SparkSession, options: Map[String, String]): Unit = ()

  override def createReader(options: Map[String, String]): Reader = new Reader {
    override def read(spark: SparkSession): DataFrame = {
      configureFileSystem(spark, options)
      val path = requiredOption(options, "path")
      val format = resolveFormat(options)
      val passThrough = options - "path" - "format"
      log.info(s"[$providerType] 读取: path=$path, format=$format, " +
        s"透传选项=${SensitiveDataMasker.maskAll(passThrough)}")
      spark.read.format(format).options(passThrough).load(path)
    }
  }

  override def createWriter(mode: String, options: Map[String, String]): Writer = new Writer {
    override def write(spark: SparkSession, df: DataFrame): Unit = {
      configureFileSystem(spark, options)
      val path = requiredOption(options, "path")
      val format = resolveFormat(options)
      val partitionCols = splitCsv(options.getOrElse("partitionBy", ""))
      val fmtOptions = options - "path" - "format" - "partitionBy"
      log.info(s"[$providerType] 写入: path=$path, format=$format, mode=$mode, " +
        s"partitionBy=${if (partitionCols.isEmpty) "-" else partitionCols.mkString(",")}")
      FileStoreLogic.stagingCommit(
        spark, df, path, format, fmtOptions, partitionCols, mode, extensionOf(format))
    }
  }

  // ------------------------------------------------------------------
  //  私有辅助
  // ------------------------------------------------------------------

  /** 读取必填选项，缺失时快速失败。 */
  protected def requiredOption(options: Map[String, String], key: String): String =
    options.getOrElse(key, throw new ConfigException(s"[$providerType] 缺少必填选项 '$key'"))

  /** 解析并校验 format 选项（默认 parquet）。 */
  private def resolveFormat(options: Map[String, String]): String = {
    val fmt = options.getOrElse("format", "parquet").toLowerCase
    if (!SupportedFormats.contains(fmt)) {
      throw new ConfigException(
        s"[$providerType] 不支持的文件格式 '$fmt'，支持: ${SupportedFormats.mkString(", ")}")
    }
    fmt
  }

  private def extensionOf(format: String): String = FormatExtensions.getOrElse(format, "")

  /** 按逗号拆分为去空白的非空列表（用于 partitionBy 等列表型选项）。 */
  protected def splitCsv(value: String): Seq[String] =
    Option(value).map(_.split(",").map(_.trim).filter(_.nonEmpty).toSeq).getOrElse(Seq.empty)
}
