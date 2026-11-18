package com.sparketl.core.config

import scala.collection.mutable.ListBuffer

/**
 * 作业配置校验器。
 *
 * 在作业提交到 Spark 集群之前执行静态校验（必填项 / 枚举值 / 组合合法性），
 * 收集全部错误后一次性报告，实现快速失败。
 */
object ConfigValidator {

  /** 内置转换类型。 */
  private val TransformKinds =
    Set("sql", "select", "rename", "cast", "filter", "dropDuplicates", "withColumn", "custom")

  /** 合法的输出写入模式。 */
  private val OutputModes = Set("append", "overwrite", "upsert")

  /** 合法的表级规则类型。 */
  private val TableRuleKinds = Set("minRows", "unique", "notNull", "sql", "custom")

  /** 合法的隔离区类型。 */
  private val QuarantineTypes = Set("parquet", "jdbc")

  /** 合法的告警通道类型。 */
  private val AlertTypes = Set("log", "webhook")

  /** 合法的重试退避策略。 */
  private val RetryPolicies = Set("fixed", "exponential")

  /** 合法的失败策略。 */
  private val OnFailurePolicies = Set("FAIL", "WARN")

  /**
   * 校验配置合法性，存在问题时抛出 [[ConfigException]]（聚合所有错误信息）。
   */
  def validate(config: JobConfig): Unit = {
    val errors = validateAll(config)
    if (errors.nonEmpty) {
      throw new ConfigException(s"作业配置校验失败，共 ${errors.size} 处错误:\n  - ${errors.mkString("\n  - ")}")
    }
  }

  /** 收集全部校验错误（空序列表示配置合法）。 */
  def validateAll(config: JobConfig): Seq[String] = {
    val errors = ListBuffer[String]()

    if (config.name == null || config.name.trim.isEmpty) {
      errors += "name: 作业名称不能为空"
    }

    // ---- 输入 ----
    if (config.input.dsType == null || config.input.dsType.trim.isEmpty) {
      errors += "input.type: 输入数据源类型不能为空"
    }

    // ---- 输出 ----
    if (config.outputs.isEmpty) {
      errors += "outputs: 至少需要一个输出目标"
    }
    config.outputs.zipWithIndex.foreach { case (o, idx) =>
      if (o.dsType == null || o.dsType.trim.isEmpty) {
        errors += s"outputs[$idx].type: 输出数据源类型不能为空"
      }
      if (!OutputModes.contains(o.mode)) {
        errors += s"outputs[$idx].mode: 非法写入模式 '${o.mode}'，合法值: ${OutputModes.mkString(", ")}"
      }
    }

    // ---- 转换 ----
    config.transforms.zipWithIndex.foreach { case (t, idx) =>
      val ctx = s"transforms[$idx]"
      if (!TransformKinds.contains(t.kind)) {
        errors += s"$ctx.kind: 非法转换类型 '${t.kind}'，合法值: ${TransformKinds.mkString(", ")}"
      }
      t.kind match {
        case "sql" =>
          requireParam(t.params, "sql", ctx, errors)
        case "select" =>
          requireParam(t.params, "columns", ctx, errors)
        case "rename" =>
          requireParam(t.params, "mappings", ctx, errors)
        case "cast" =>
          requireParam(t.params, "mappings", ctx, errors)
        case "filter" =>
          requireParam(t.params, "condition", ctx, errors)
        case "withColumn" =>
          requireParam(t.params, "name", ctx, errors)
          requireParam(t.params, "expression", ctx, errors)
        case "custom" =>
          if (t.name.forall(_.trim.isEmpty)) {
            errors += s"$ctx.name: custom 转换必须提供 SPI 注册名"
          }
        case _ => // kind 非法的错误已记录
      }
    }

    // ---- 质量 ----
    config.quality.foreach { q =>
      q.rowRules.zipWithIndex.foreach { case (r, idx) =>
        if (r.name == null || r.name.trim.isEmpty) errors += s"quality.rowRules[$idx].name: 规则名不能为空"
        if (r.expression == null || r.expression.trim.isEmpty) {
          errors += s"quality.rowRules[$idx].expression: 规则表达式不能为空"
        }
      }
      q.tableRules.zipWithIndex.foreach { case (r, idx) =>
        val ctx = s"quality.tableRules[$idx]"
        if (r.name == null || r.name.trim.isEmpty) errors += s"$ctx.name: 规则名不能为空"
        if (!TableRuleKinds.contains(r.kind)) {
          errors += s"$ctx.kind: 非法规则类型 '${r.kind}'，合法值: ${TableRuleKinds.mkString(", ")}"
        }
        r.kind match {
          case "minRows"  => requireParam(r.params, "minRows", ctx, errors)
          case "unique"   => requireParam(r.params, "columns", ctx, errors)
          case "notNull"  => requireParam(r.params, "columns", ctx, errors)
          case "sql"      => requireParam(r.params, "sql", ctx, errors)
          case "custom"   => requireParam(r.params, "rule", ctx, errors)
          case _          => // kind 非法的错误已记录
        }
      }
      if (!OnFailurePolicies.contains(q.onFailure)) {
        errors += s"quality.onFailure: 非法策略 '${q.onFailure}'，合法值: FAIL | WARN"
      }
      q.quarantine.foreach { quarantine =>
        if (!QuarantineTypes.contains(quarantine.dsType)) {
          errors += s"quality.quarantine.type: 非法类型 '${quarantine.dsType}'，合法值: ${QuarantineTypes.mkString(", ")}"
        }
        if (quarantine.dsType == "parquet" && quarantine.path.forall(_.trim.isEmpty)) {
          errors += "quality.quarantine.path: parquet 隔离区必须提供 path"
        }
        if (quarantine.dsType == "jdbc" && !quarantine.options.contains("url")) {
          errors += "quality.quarantine.options.url: jdbc 隔离区必须提供 url"
        }
      }
      q.alerts.zipWithIndex.foreach { case (a, idx) =>
        if (!AlertTypes.contains(a.dsType)) {
          errors += s"quality.alerts[$idx].type: 非法告警类型 '${a.dsType}'，合法值: ${AlertTypes.mkString(", ")}"
        }
        if (a.dsType == "webhook" && a.url.forall(_.trim.isEmpty)) {
          errors += s"quality.alerts[$idx].url: webhook 告警必须提供 url"
        }
      }
    }

    // ---- 重试 ----
    config.retry.foreach { r =>
      if (r.maxAttempts < 1) {
        errors += s"retry.maxAttempts: 必须大于等于 1，实际为 ${r.maxAttempts}"
      }
      if (!RetryPolicies.contains(r.policy)) {
        errors += s"retry.policy: 非法策略 '${r.policy}'，合法值: ${RetryPolicies.mkString(", ")}"
      }
      if (r.initialDelayMs < 0) {
        errors += s"retry.initialDelayMs: 不能为负数，实际为 ${r.initialDelayMs}"
      }
      if (r.multiplier <= 0) {
        errors += s"retry.multiplier: 必须为正数，实际为 ${r.multiplier}"
      }
    }

    // ---- 监控 ----
    config.monitor.foreach { m =>
      m.runStore.foreach { rs =>
        if (rs.url == null || rs.url.trim.isEmpty) {
          errors += "monitor.runStore.url: 运行状态库 JDBC URL 不能为空"
        }
      }
      m.metrics.foreach { mc =>
        mc.prometheusPort.foreach { p =>
          if (p < 1 || p > 65535) {
            errors += s"monitor.metrics.prometheusPort: 端口必须在 1~65535 之间，实际为 $p"
          }
        }
      }
    }

    errors.toList
  }

  private def requireParam(
      params: Map[String, String],
      key: String,
      ctx: String,
      errors: ListBuffer[String]
  ): Unit =
    params.get(key).forall(_.trim.isEmpty) && {
      errors += s"$ctx.params.$key: 必填参数缺失或为空"
      false
    }
}
