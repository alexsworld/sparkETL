package com.sparketl.connectors.iceberg

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import com.sparketl.core.config.ConfigException
import com.sparketl.core.spi.{DataSourceProvider, Reader, Writer}
import com.sparketl.core.util.SensitiveDataMasker

/**
 * Iceberg 连接器（providerType = "iceberg"）。
 *
 * 运行时依赖：iceberg-spark-runtime 未随本框架打包，需用户通过
 * spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:x.y.z
 * 提供；表标识需与 IcebergCatalog 配置（spark.sql.catalog.*）对应。
 *
 * 读取选项：整体透传给 spark.read（如 catalog / 表标识等）；
 * options 含 path 时按路径加载，否则无参加载（表标识等由选项透传解析）。
 *
 * 写入选项：
 *  - 目标：path 或 table（二选一必填）；
 *  - mode: append -> append；
 *    overwrite -> options 含 replaceWhere 时按谓词覆盖，否则全量覆盖；
 *  - 提交由 Iceberg 自身事务保证原子性（快照提交），无需外部 staging。
 */
class IcebergProvider extends DataSourceProvider {

  private val log = LoggerFactory.getLogger(classOf[IcebergProvider])

  override def providerType: String = "iceberg"

  override def createReader(options: Map[String, String]): Reader = new Reader {
    override def read(spark: SparkSession): DataFrame = {
      log.info(s"[iceberg] 读取: ${SensitiveDataMasker.maskAll(options)}")
      val reader = spark.read.format("iceberg").options(options)
      if (options.contains("path")) reader.load(options("path")) else reader.load()
    }
  }

  override def createWriter(mode: String, options: Map[String, String]): Writer = new Writer {
    override def write(spark: SparkSession, df: DataFrame): Unit = {
      val target = options.getOrElse("path", options.getOrElse("table",
        throw new ConfigException("[iceberg] 写入需要配置 'path' 或 'table' 选项之一")))
      log.info(s"[iceberg] 写入: target=$target, mode=$mode, " +
        s"选项=${SensitiveDataMasker.maskAll(options - "path" - "table")}")
      val writer = df.write.format("iceberg")
      mode match {
        case "append" =>
          writer.mode("append")
        case "overwrite" =>
          options.get("replaceWhere") match {
            case Some(replaceWhere) =>
              log.info(s"[iceberg] 按谓词覆盖: replaceWhere=$replaceWhere")
              writer.option("replaceWhere", replaceWhere).mode("overwrite")
            case None =>
              writer.mode("overwrite")
          }
        case other =>
          throw new ConfigException(
            s"[iceberg] 不支持的写入模式 '$other'（支持 append | overwrite）")
      }
      writer.save(target)
    }
  }
}
