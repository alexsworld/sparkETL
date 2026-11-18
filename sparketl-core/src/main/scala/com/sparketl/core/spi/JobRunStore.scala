package com.sparketl.core.spi

import java.time.LocalDateTime

/**
 * 作业运行记录（写入运行状态库，用于作业状态追踪与可视化）。
 *
 * @param runId          运行唯一标识（jobName-时间戳-随机后缀）
 * @param jobName        作业名称
 * @param batchTime      批次时间
 * @param status         状态：RUNNING | SUCCEEDED | FAILED
 * @param startTime      开始时间
 * @param endTime        结束时间（运行中为 None）
 * @param inputRows      输入行数
 * @param outputRows     输出行数
 * @param quarantinedRows 质量隔离行数
 * @param errorMessage   失败信息（成功时为 None）
 */
case class JobRunRecord(
    runId: String,
    jobName: String,
    batchTime: String,
    status: String,
    startTime: LocalDateTime,
    endTime: Option[LocalDateTime] = None,
    inputRows: Long = 0L,
    outputRows: Long = 0L,
    quarantinedRows: Long = 0L,
    errorMessage: Option[String] = None
)

/** 作业状态常量。 */
object JobStatus {
  val Running   = "RUNNING"
  val Succeeded = "SUCCEEDED"
  val Failed    = "FAILED"
}

/**
 * 作业运行状态存储 SPI。
 *
 * 生产实现（JDBC 持久化）位于 sparketl-monitor 模块，
 * 通过 [[JobRunStoreProvider]] 经 ServiceLoader 创建。
 */
trait JobRunStore {

  /** 记录作业启动。 */
  def start(record: JobRunRecord): Unit

  /** 记录作业成功。 */
  def success(record: JobRunRecord): Unit

  /** 记录作业失败。 */
  def failure(record: JobRunRecord): Unit
}

/**
 * 作业运行状态存储工厂 SPI。
 *
 * 实现类通过 Java ServiceLoader 注册，按 `storeType` 路由
 * （当前内置 jdbc 类型，见 sparketl-monitor 模块）。
 */
trait JobRunStoreProvider {

  /** 存储类型标识，对应 YAML `monitor.runStore` 段。 */
  def storeType: String

  /** 根据运行状态库配置创建存储实例。 */
  def create(config: RunStoreConfigHolder): JobRunStore
}

/**
 * 运行状态库配置载体。
 *
 * 独立于 com.sparketl.core.config.RunStoreConfig，避免 SPI 实现方
 * 与配置模型产生双向编译依赖；字段语义一致。
 */
final case class RunStoreConfigHolder(
    url: String,
    user: Option[String] = None,
    password: Option[String] = None,
    table: Option[String] = None
)
