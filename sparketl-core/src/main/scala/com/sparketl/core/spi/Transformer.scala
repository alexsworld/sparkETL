package com.sparketl.core.spi

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 数据转换器 SPI。
 *
 * 内置转换（SQL / 字段操作 / 过滤 / 去重等）与自定义转换均实现此接口；
 * 自定义转换器通过 Java ServiceLoader 注册（META-INF/services），
 * 在 YAML 中以 `kind: custom` + `name: 注册名` 引用。
 */
trait Transformer {

  /** 转换器名称（内置转换为 kind 名称，自定义转换为 SPI 注册名）。 */
  def name: String

  /**
   * 执行转换。
   *
   * @param spark  Spark 会话
   * @param df     输入 DataFrame
   * @param params 转换参数（来自 YAML transforms[].params）
   * @return 转换后的 DataFrame
   */
  def transform(spark: SparkSession, df: DataFrame, params: Map[String, String]): DataFrame
}
