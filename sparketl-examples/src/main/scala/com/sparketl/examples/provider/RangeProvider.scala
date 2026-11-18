package com.sparketl.examples.provider

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.lit

import com.sparketl.core.spi.{DataSourceProvider, Reader, Writer}

/**
 * 自定义数据源连接器示例：`range` 生成器数据源。
 *
 * 演示如何实现 DataSourceProvider SPI 并接入框架：
 *  - 读：按 options 生成 [start, end) 的自增整数序列（id 列），附 batch_time 列；
 *  - 写：以 JSON 格式写到 options.path 指定的目录。
 *
 * 注册方式（ServiceLoader）：
 * `META-INF/services/com.sparketl.core.spi.DataSourceProvider` 声明本类全限定名，
 * YAML 中引用：
 * {{{
 * input:
 *   type: range
 *   options: { start: "0", end: "1000" }
 * }}}
 */
class RangeProvider extends DataSourceProvider {

  /** 连接器类型标识，与 YAML `type` 对应。 */
  override def providerType: String = "range"

  override def createReader(options: Map[String, String]): Reader = new Reader {
    override def read(spark: SparkSession): DataFrame = {
      val start = options.get("start").map(_.toLong).getOrElse(0L)
      val end = options.get("end").map(_.toLong)
        .getOrElse(throw new IllegalArgumentException("range 数据源缺少必填选项: end"))
      spark.range(start, end).withColumn("batch_time", lit(""))
    }
  }

  override def createWriter(mode: String, options: Map[String, String]): Writer = new Writer {
    override def write(spark: SparkSession, df: DataFrame): Unit = {
      val path = options.getOrElse("path",
        throw new IllegalArgumentException("range 数据源写入缺少必填选项: path"))
      df.write.mode(mode).format("json").save(path)
    }
  }
}
