package com.sparketl.core.lifecycle

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.slf4j.LoggerFactory

import com.sparketl.core.config.JobConfig
import com.sparketl.core.exception.JobPhase
import com.sparketl.core.metrics.EtlMetrics
import com.sparketl.core.spi.{JobRunRecord, JobRunStore, JobStatus}

/**
 * 单次作业运行的上下文：贯穿生命周期各阶段，承载运行标识、
 * 指标上报入口、状态存储与行数统计。
 *
 * @param config    作业配置
 * @param runId     本次运行唯一标识
 * @param batchTime 批次时间
 * @param metrics   指标上报器
 */
class JobContext(
    val config: JobConfig,
    val runId: String,
    val batchTime: String,
    val metrics: EtlMetrics
) {

  private val log = LoggerFactory.getLogger(classOf[JobContext])

  private[core] var runStore: Option[JobRunStore] = None

  @volatile private[core] var currentPhase: String = JobPhase.Validate

  @volatile private var inputRows: Long = 0L
  @volatile private var outputRows: Long = 0L
  @volatile private var quarantinedRows: Long = 0L

  private val startTime: LocalDateTime = LocalDateTime.now()

  /** 当前所处生命周期阶段。 */
  def phase: String = currentPhase

  /** 作业开始时间。 */
  def getStartTime: LocalDateTime = startTime

  def getInputRows: Long = inputRows
  def getOutputRows: Long = outputRows
  def getQuarantinedRows: Long = quarantinedRows

  private[core] def setInputRows(n: Long): Unit = {
    inputRows = n
    metrics.gauge("etl.input.rows", n)
  }

  private[core] def setOutputRows(n: Long): Unit = {
    outputRows = n
    metrics.gauge("etl.output.rows", n)
  }

  private[core] def setQuarantinedRows(n: Long): Unit = {
    quarantinedRows = n
    metrics.gauge("etl.quarantined.rows", n)
  }

  /** 构造运行记录快照（错误信息已脱敏）。 */
  def record(status: String, errorMessage: Option[String] = None): JobRunRecord =
    JobRunRecord(
      runId = runId,
      jobName = config.name,
      batchTime = batchTime,
      status = status,
      startTime = startTime,
      endTime = if (status == JobStatus.Running) None else Some(LocalDateTime.now()),
      inputRows = inputRows,
      outputRows = outputRows,
      quarantinedRows = quarantinedRows,
      errorMessage = errorMessage.map(SensitiveMessage.sanitize)
    )

  /** 标记阶段并记录日志。 */
  private[core] def enterPhase(phase: String): Unit = {
    currentPhase = phase
    log.info("[{}] 作业 {} 进入 {} 阶段", runId, config.name, phase)
  }
}

/** 运行记录脱敏工具：错误信息中可能包含连接串口令等敏感内容。 */
object SensitiveMessage {

  /** 截断过长错误信息并掩去连接串中的口令参数。 */
  def sanitize(message: String): String = {
    val masked = message
      .replaceAll("(?i)(password=)[^;\\s&]+", "$1***")
      .replaceAll("(?i)(jdbc:[^?\\s]+\\?)[^\\s]+", "$1***")
    if (masked.length > 2000) masked.substring(0, 2000) + "...(截断)" else masked
  }
}
