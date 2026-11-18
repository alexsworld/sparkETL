package com.sparketl.examples.rule

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

import com.sparketl.core.config.ConfigException
import com.sparketl.core.spi.{QualityCheckResult, QualityRule}

/**
 * 自定义表级质量规则示例：数值列范围校验。
 *
 * 注册方式（ServiceLoader）：
 * `META-INF/services/com.sparketl.core.spi.QualityRule` 声明本类全限定名，
 * YAML 中引用：
 * {{{
 * quality:
 *   tableRules:
 *     - name: age_range
 *       kind: custom
 *       params: { rule: "columnRange", column: "age", min: "0", max: "150" }
 * }}}
 */
class ColumnRangeRule extends QualityRule {

  /** SPI 注册名，与 YAML `tableRules[].params.rule` 对应。 */
  override def name: String = "columnRange"

  override def evaluate(spark: SparkSession, df: DataFrame, params: Map[String, String]): QualityCheckResult = {
    val column = required(params, "column")
    val min = params.get("min").map(_.toDouble)
    val max = params.get("max").map(_.toDouble)

    val target = col(column)
    val rangeConds =
      min.map(target < _).toSeq ++ max.map(target > _).toSeq
    val outOfRange = df.filter(target.isNotNull && rangeConds.reduce(_ || _))

    val violations = outOfRange.count()
    QualityCheckResult(
      ruleName = s"$name($column)",
      passed = violations == 0L,
      metric = violations,
      message = if (violations == 0L) s"列 $column 全部在合法范围内"
      else s"列 $column 有 $violations 行超出范围 [${min.getOrElse("-inf")}, ${max.getOrElse("+inf")}]")
  }

  private def required(params: Map[String, String], key: String): String =
    params.get(key).map(_.trim).filter(_.nonEmpty).getOrElse(
      throw new ConfigException(s"columnRange 规则缺少必填参数 '$key'"))
}
