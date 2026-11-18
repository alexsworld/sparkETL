package com.sparketl.core.config

/**
 * 作业配置强类型模型集合。
 *
 * 所有 case class 的字段与 YAML 键保持 snake_case 对应关系；
 * 解析逻辑见 [[ConfigParser]]，合法性校验见 [[ConfigValidator]]。
 */

/**
 * 顶层作业配置。
 *
 * @param name       作业名称（必填，全局唯一）
 * @param batchTime  批次时间（可选，缺省为运行时当前时间）
 * @param spark      Spark 参数覆盖（作业级，优先级最高）
 * @param input      输入数据源配置
 * @param outputs    输出数据源配置（一个或多个，复用同一份数据）
 * @param transforms 转换链（按顺序执行）
 * @param quality    数据质量校验配置（可选）
 * @param retry      重试策略配置（可选）
 * @param monitor    监控配置（可选）
 */
final case class JobConfig(
    name: String,
    batchTime: Option[String],
    spark: Option[Map[String, String]],
    input: InputConfig,
    outputs: Seq[OutputConfig],
    transforms: Seq[TransformConfig],
    quality: Option[QualityConfig],
    retry: Option[RetryConfig],
    monitor: Option[MonitorConfig]
)

/** 输入数据源配置：dsType 对应 YAML `type`，为连接器类型。 */
final case class InputConfig(
    dsType: String,
    options: Map[String, String] = Map.empty
)

/**
 * 输出数据源配置。
 *
 * @param dsType  连接器类型（对应 YAML `type`）
 * @param mode    写入模式：append | overwrite
 * @param options 连接器选项
 */
final case class OutputConfig(
    dsType: String,
    mode: String,
    options: Map[String, String] = Map.empty
)

/**
 * 单个转换步骤配置。
 *
 * @param kind   转换类型：sql | select | rename | cast | filter |
 *               dropDuplicates | withColumn | custom
 * @param name   custom 类型时的自定义转换器名称（SPI 注册名）
 * @param params 转换参数
 */
final case class TransformConfig(
    kind: String,
    name: Option[String] = None,
    params: Map[String, String] = Map.empty
)

/** 行级质量规则：expression 为 Spark SQL 布尔表达式，为真表示合规。 */
final case class RowRule(name: String, expression: String)

/**
 * 表级质量规则。
 *
 * @param kind   规则类型：minRows | unique | notNull | sql | custom
 * @param params 规则参数
 */
final case class TableRule(name: String, kind: String, params: Map[String, String] = Map.empty)

/** 隔离区配置：dsType 为 parquet 或 jdbc（对应 YAML `type`）。 */
final case class QuarantineConfig(
    dsType: String,
    path: Option[String],
    options: Map[String, String] = Map.empty
)

/** 告警通道配置：dsType 为 log 或 webhook（对应 YAML `type`，webhook 需提供 url）。 */
final case class AlertConfig(dsType: String, url: Option[String])

/**
 * 数据质量校验配置。
 *
 * @param onFailure  表级规则失败时的策略：FAIL（阻断作业）| WARN（记录后继续）
 * @param quarantine 隔离区配置（行级违规数据写入，可选）
 * @param alerts     告警通道列表
 */
final case class QualityConfig(
    rowRules: Seq[RowRule] = Seq.empty,
    tableRules: Seq[TableRule] = Seq.empty,
    onFailure: String = "WARN",
    quarantine: Option[QuarantineConfig] = None,
    alerts: Seq[AlertConfig] = Seq.empty
)

/**
 * 重试策略配置。
 *
 * @param maxAttempts    最大尝试次数（含首次）
 * @param policy         退避策略：fixed | exponential
 * @param initialDelayMs 首次重试延迟（毫秒）
 * @param multiplier     exponential 策略的退避倍数
 * @param jitter         是否加入随机抖动，避免重试雪崩
 */
final case class RetryConfig(
    maxAttempts: Int = 3,
    policy: String = "fixed",
    initialDelayMs: Long = 1000L,
    multiplier: Double = 2.0,
    jitter: Boolean = true
)

/** 运行状态库配置（JDBC 持久化作业运行记录）。 */
final case class RunStoreConfig(
    url: String,
    user: Option[String] = None,
    password: Option[String] = None,
    table: Option[String] = None
)

/** 指标输出配置：Prometheus HTTP 端口与/或 SLF4J 周期上报。 */
final case class MetricsConfig(
    prometheusPort: Option[Int] = None,
    slf4j: Option[Boolean] = None
)

/** 监控配置：运行状态库 + 指标输出。 */
final case class MonitorConfig(
    runStore: Option[RunStoreConfig] = None,
    metrics: Option[MetricsConfig] = None
)
