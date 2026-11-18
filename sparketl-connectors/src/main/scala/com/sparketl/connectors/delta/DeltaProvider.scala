package com.sparketl.connectors.delta

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import com.sparketl.core.config.ConfigException
import com.sparketl.core.spi.{DataSourceProvider, Reader, Writer}
import com.sparketl.core.util.SensitiveDataMasker

/**
 * Delta Lake 连接器（providerType = "delta"）。
 *
 * 运行时依赖：delta-spark 未随本框架打包，需用户通过
 * spark-submit --packages io.delta:delta-spark_2.12:3.1.0 提供，
 * 并配置 spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension 与
 * spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog。
 *
 * 读取选项：整体透传给 spark.read（如 versionAsOf / timestampAsOf 时间旅行等）；
 * options 含 path 时按路径加载，否则无参加载（表名等由选项透传解析）。
 *
 * 写入选项：
 *  - 目标：path 或 table（二选一必填）；
 *  - mode: append -> append；
 *    overwrite -> options 含 replaceWhere 时按谓词覆盖，否则全量覆盖；
 *  - 提交由 Delta 自身事务（_delta_log）保证原子性，无需外部 staging。
 */
class DeltaProvider extends DataSourceProvider {

  private val log = LoggerFactory.getLogger(classOf[DeltaProvider])

  override def providerType: String = "delta"

  override def createReader(options: Map[String, String]): Reader = new Reader {
    override def read(spark: SparkSession): DataFrame = {
      log.info(s"[delta] 读取: ${SensitiveDataMasker.maskAll(options)}")
      val reader = spark.read.format("delta").options(options)
      if (options.contains("path")) reader.load(options("path")) else reader.load()
    }
  }

  override def createWriter(mode: String, options: Map[String, String]): Writer = new Writer {
    override def write(spark: SparkSession, df: DataFrame): Unit = {
      val target = options.getOrElse("path", options.getOrElse("table",
        throw new ConfigException("[delta] 写入需要配置 'path' 或 'table' 选项之一")))
      log.info(s"[delta] 写入: target=$target, mode=$mode, " +
        s"选项=${SensitiveDataMasker.maskAll(options - "path" - "table")}")
      val writer = df.write.format("delta")
      mode match {
        case "append" =>
          writer.mode("append")
        case "overwrite" =>
          options.get("replaceWhere") match {
            case Some(replaceWhere) =>
              log.info(s"[delta] 按谓词覆盖: replaceWhere=$replaceWhere")
              writer.option("replaceWhere", replaceWhere).mode("overwrite")
            case None =>
              writer.mode("overwrite")
          }
        case other =>
          throw new ConfigException(
            s"[delta] 不支持的写入模式 '$other'（支持 append | overwrite）")
      }
      writer.save(target)
    }
  }
}
