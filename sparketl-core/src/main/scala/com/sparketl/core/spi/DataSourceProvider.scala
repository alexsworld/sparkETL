package com.sparketl.core.spi

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 数据源读取器 SPI：从目标存储系统读取数据并返回 DataFrame。
 *
 * 实现类应保持无状态，read 可被重试机制重复调用（幂等）。
 */
trait Reader {

  /**
   * 读取数据源。
   *
   * @param spark Spark 会话
   * @return 读取到的 DataFrame（惰性，未触发执行）
   */
  def read(spark: SparkSession): DataFrame
}

/**
 * 数据源写入器 SPI：将 DataFrame 写入目标存储系统。
 *
 * 实现类应尽量保证幂等性（重跑不产生重复数据），例如：
 * staging + 原子提交、动态分区覆盖、upsert、表格式原生事务等。
 */
trait Writer {

  /**
   * 写入数据。
   *
   * @param spark Spark 会话
   * @param df    待写入数据（已转换、已质检）
   */
  def write(spark: SparkSession, df: DataFrame): Unit
}

/**
 * 数据源提供者 SPI（连接器入口）。
 *
 * 通过 Java ServiceLoader 注册：在
 * `META-INF/services/com.sparketl.core.spi.DataSourceProvider`
 * 中声明实现类全限定名，框架启动时自动加载并按 `providerType` 路由。
 */
trait DataSourceProvider {

  /** 连接器类型标识（与 YAML `input.type` / `outputs[].type` 对应），如 jdbc、hive。 */
  def providerType: String

  /** 创建读取器。 */
  def createReader(options: Map[String, String]): Reader

  /**
   * 创建写入器。
   *
   * @param mode 写入模式：append | overwrite | upsert
   */
  def createWriter(mode: String, options: Map[String, String]): Writer
}
