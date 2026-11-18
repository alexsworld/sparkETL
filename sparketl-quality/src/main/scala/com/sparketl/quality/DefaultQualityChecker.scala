package com.sparketl.quality

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{coalesce, col, expr, lit, not, when}
import org.slf4j.LoggerFactory

import com.sparketl.core.config.{ConfigException, QualityConfig, TableRule}
import com.sparketl.core.spi.{QualityCheckResult, QualityChecker, QualityOutcome, QualityRule}
import com.sparketl.quality.alert.AlertDispatcher
import com.sparketl.quality.quarantine.QuarantineWriter
import com.sparketl.quality.rules.{BuiltInTableRules, CustomRuleLoader}

/**
 * 质量校验器默认实现：行级分流 + 表级校验 + 隔离区写入 + 告警的整体编排。
 *
 * 处理流程：
 *  1. 行级规则（rowRules）：expression 为真的行视为合规；违规行分流到隔离数据集，
 *     并附 `etl_quarantine_rule`（首个违规规则名）溯源列；规则表达式为 null 的行
 *     按不合规处理（宁可错杀，不放过脏数据）。
 *  2. 隔离区（quarantine）：配置了隔离区且存在违规行时写入（写失败仅告警不阻断）。
 *  3. 表级规则（tableRules）：按 kind 匹配内置规则或 SPI 自定义规则后逐条校验。
 *  4. 告警（alerts）：存在表级未通过项时分发告警。
 *  5. 阻断（blocked）：仅当 onFailure=FAIL 且表级存在未通过项时为 true。
 *
 * 行级违规不算 failure（只隔离不上报阻断），failures 仅包含表级未通过结果。
 */
class DefaultQualityChecker extends QualityChecker {

  private val log = LoggerFactory.getLogger(getClass)

  private val ValidCol = "__etl_valid"
  private val QuarantineRuleCol = "etl_quarantine_rule"

  override def applyQuality(
      spark: SparkSession,
      df: DataFrame,
      config: QualityConfig,
      batchTime: String
  ): QualityOutcome = {
    val total = df.count()
    if (config.rowRules.isEmpty && config.tableRules.isEmpty) {
      return QualityOutcome(df, 0L, total, Nil, blocked = false)
    }

    // ---- 1. 行级分流 ----
    var cleanDf = df
    var quarantined = 0L
    if (config.rowRules.nonEmpty) {
      val ruleColumns: Seq[(String, Column)] = config.rowRules.map(r => r.name -> expr(r.expression))
      // 合规判定：所有规则 AND；表达式为 null 视为不合规
      val validExpr = ruleColumns.map(_._2).reduce(_ and _)
      // 首个违规规则名：按规则声明顺序取第一个不满足者
      val firstFailExpr = coalesce(
        ruleColumns.map { case (name, c) => when(not(c), lit(name)) }.reduce(_ or _),
        lit("unknown"))

      val flagged = df
        .withColumn(QuarantineRuleCol, firstFailExpr)
        .withColumn(ValidCol, coalesce(validExpr, lit(false)))

      cleanDf = flagged.filter(col(ValidCol)).drop(ValidCol, QuarantineRuleCol)
      val quarantinedDf = flagged.filter(not(col(ValidCol))).drop(ValidCol)
      quarantined = quarantinedDf.count()

      config.quarantine.foreach { q =>
        if (quarantined > 0L) {
          QuarantineWriter.write(spark, quarantinedDf, q, batchTime, Nil)
        }
      }
      log.info(s"行级规则分流完成: total=$total, clean=${total - quarantined}, quarantined=$quarantined")
    }

    // ---- 2. 表级校验 ----
    val tableResults: Seq[QualityCheckResult] = config.tableRules.map(rule => evaluateTableRule(spark, df, rule))
    val tableFailures = tableResults.filterNot(_.passed)

    // ---- 3. 告警 ----
    if (tableFailures.nonEmpty) {
      AlertDispatcher.dispatch(config.alerts, "quality-check", tableFailures)
    }

    // ---- 4. 阻断判定 ----
    val blocked = (config.onFailure == "FAIL") && tableFailures.nonEmpty

    log.info(s"质量校验完成: total=$total, quarantined=$quarantined, 表级失败数=${tableFailures.size}, blocked=$blocked")
    QualityOutcome(cleanDf, quarantined, total, tableFailures, blocked)
  }

  /**
   * 执行单条表级规则：kind=custom 走 SPI 加载，其余匹配内置规则。
   *
   * 规则执行异常（含配置缺失、未知类型）以 ConfigException 快速失败，
   * 不转换为失败结果，避免配置错误被静默吞掉。
   */
  private def evaluateTableRule(spark: SparkSession, df: DataFrame, rule: TableRule): QualityCheckResult = {
    val ruleImpl: QualityRule =
      if (rule.kind == "custom") {
        val customName = rule.params.getOrElse("rule",
          throw new ConfigException(s"表级规则 '${rule.name}' 为 custom 类型，缺少 params.rule"))
        CustomRuleLoader.load(customName)
      } else {
        BuiltInTableRules.byKind(rule.kind).getOrElse(
          throw new ConfigException(s"未知的表级规则类型 '${rule.kind}'（规则 '${rule.name}'）"))
      }
    ruleImpl.evaluate(spark, df, rule.params)
  }
}
