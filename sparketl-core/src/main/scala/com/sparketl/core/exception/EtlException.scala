package com.sparketl.core.exception

/**
 * ETL 框架统一异常基类，携带作业执行阶段信息，便于定位失败环节。
 *
 * @param phase   失败时所处的生命周期阶段
 * @param message 错误描述
 * @param cause   根因
 */
class EtlException(val phase: String, message: String, cause: Throwable = null)
  extends RuntimeException(s"[$phase] $message", cause)

/** 生命周期阶段常量。 */
object JobPhase {
  val Validate  = "VALIDATE"
  val Init      = "INIT"
  val Extract   = "EXTRACT"
  val Transform = "TRANSFORM"
  val Quality   = "QUALITY"
  val Load      = "LOAD"
  val Finalize  = "FINALIZE"
  val Cleanup   = "CLEANUP"
}

/** 表级质量规则在 FAIL 策略下阻断作业时抛出。 */
class QualityBlockException(message: String, cause: Throwable = null)
  extends EtlException(JobPhase.Quality, message, cause)
