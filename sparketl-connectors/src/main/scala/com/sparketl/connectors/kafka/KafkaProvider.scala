package com.sparketl.connectors.kafka

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import com.sparketl.core.config.ConfigException
import com.sparketl.core.spi.{DataSourceProvider, Reader, Writer}
import com.sparketl.core.util.SensitiveDataMasker

/**
 * Kafka 连接器（providerType = "kafka"），基于 Spark Kafka 批数据源（batch）。
 *
 * 运行时依赖：spark-sql-kafka-0-10 未随本框架打包，需用户通过
 * spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 提供。
 *
 * 读取选项：
 *  - bootstrapServers（必填）：映射为 kafka.bootstrap.servers；
 *  - 其余选项透传（subscribe / assign / startingOffsets / endingOffsets 等）。
 *
 * 写入选项：
 *  - bootstrapServers（必填，映射 kafka.bootstrap.servers）、topic（必填）；
 *  - 要求 DataFrame 含 key / value 列，写入模式固定 append。
 */
class KafkaProvider extends DataSourceProvider {

  private val log = LoggerFactory.getLogger(classOf[KafkaProvider])

  override def providerType: String = "kafka"

  override def createReader(options: Map[String, String]): Reader = new Reader {
    override def read(spark: SparkSession): DataFrame = {
      val bootstrap = options.getOrElse("bootstrapServers",
        throw new ConfigException("[kafka] 读取缺少必填选项 'bootstrapServers'"))
      val passThrough = options - "bootstrapServers"
      log.info(s"[kafka] 批量读取: bootstrapServers=$bootstrap, " +
        s"透传选项=${SensitiveDataMasker.maskAll(passThrough)}")
      spark.read.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .options(passThrough)
        .load()
    }
  }

  override def createWriter(mode: String, options: Map[String, String]): Writer = new Writer {
    override def write(spark: SparkSession, df: DataFrame): Unit = {
      val bootstrap = options.getOrElse("bootstrapServers",
        throw new ConfigException("[kafka] 写入缺少必填选项 'bootstrapServers'"))
      val topic = options.getOrElse("topic",
        throw new ConfigException("[kafka] 写入缺少必填选项 'topic'"))
      val columns = df.columns.toSet
      if (!columns.contains("key") || !columns.contains("value")) {
        throw new ConfigException(s"[kafka] 写入要求 DataFrame 包含 key / value 列，" +
          s"当前列: ${df.columns.mkString(", ")}")
      }
      log.info(s"[kafka] 写入: bootstrapServers=$bootstrap, topic=$topic")
      df.select("key", "value").write.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("topic", topic)
        .mode("append")
        .save()
    }
  }
}
