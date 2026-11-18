package com.sparketl.core.spi

import org.apache.spark.sql.{DataFrame, SparkSession}

import com.sparketl.core.config.QualityConfig

/**
 * 单条质量规则的校验结果。
 *
 * @param ruleName 规则名称
 * @param passed   是否通过
 * @param metric   关键度量（违规行数 / 实际行数 / 重复数等，视规则而定）
 * @param message  结果描述（失败时说明原因）
 */
case class QualityCheckResult(ruleName: String, passed: Boolean, metric: Long, message: String)

/**
 * 质量校验整体输出。
 *
 * @param cleanDf     合规数据（可直接进入 Load 阶段）
 * @param quarantined 隔离的违规行数
 * @param total       参与校验的总行数
 * @param failures    未通过的校验结果（含行级与表级）
 * @param blocked     是否阻断作业（FAIL 策略下表级规则失败时为 true）
 */
case class QualityOutcome(
    cleanDf: DataFrame,
    quarantined: Long,
    total: Long,
    failures: Seq[QualityCheckResult],
    blocked: Boolean
)

/**
 * 单条表级质量规则 SPI。
 *
 * 内置规则（minRows / unique / notNull / sql）与自定义规则均实现此接口；
 * 自定义规则通过 Java ServiceLoader 注册，YAML 中以
 * `kind: custom` + `params.rule: 注册名` 引用。
 */
trait QualityRule {

  /** 规则名称（内置规则为 kind 名称，自定义规则为 SPI 注册名）。 */
  def name: String

  /**
   * 执行校验。
   *
   * @param spark  Spark 会话
   * @param df     待校验数据
   * @param params 规则参数（来自 YAML tableRules[].params）
   * @return 校验结果
   */
  def evaluate(spark: SparkSession, df: DataFrame, params: Map[String, String]): QualityCheckResult
}

/**
 * 质量校验器 SPI：行级分流 + 表级校验 + 隔离区写入 + 告警的整体编排入口。
 *
 * 默认实现位于 sparketl-quality 模块（[[com.sparketl.quality.DefaultQualityChecker]]），
 * 通过 Java ServiceLoader 加载；未配置 quality 段时生命周期直接跳过。
 */
trait QualityChecker {

  /**
   * 执行质量校验。
   *
   * @param spark     Spark 会话
   * @param df        待校验数据
   * @param config    质量配置
   * @param batchTime 批次时间（写入隔离区附带的溯源字段）
   * @return 校验结果（合规数据 + 隔离数据 + 阻断标志）
   */
  def applyQuality(
      spark: SparkSession,
      df: DataFrame,
      config: QualityConfig,
      batchTime: String
  ): QualityOutcome
}
