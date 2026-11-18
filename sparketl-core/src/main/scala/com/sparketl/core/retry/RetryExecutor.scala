package com.sparketl.core.retry

import scala.util.Random
import scala.util.control.NonFatal

import org.slf4j.LoggerFactory

import com.sparketl.core.config.RetryConfig

/**
 * 重试执行器：包装资源初始化与 Extract 等易受瞬时故障影响的操作。
 *
 * 特性：
 *  - fixed / exponential 两种退避策略，exponential 以 multiplier 指数递增；
 *  - 可选随机抖动（±20%），避免大规模重试同时发起造成雪崩；
 *  - 仅对"可重试异常"重试（连接类异常白名单）。
 */
object RetryExecutor {

  private val log = LoggerFactory.getLogger(getClass)

  /** 可重试异常白名单（按类名匹配，含子类）。 */
  private val RetryableClasses: Seq[String] = Seq(
    "java.sql.SQLException",
    "java.net.ConnectException",
    "java.net.SocketTimeoutException",
    "java.net.UnknownHostException",
    "java.io.EOFException",
    "java.io.IOException",
    "org.apache.hadoop.hive.metastore.api.MetaException"
  )

  /** 默认可重试判定：命中白名单类名（含子类）即视为瞬时故障。 */
  val defaultRetryable: Throwable => Boolean = (t: Throwable) =>
    RetryableClasses.exists { name =>
      try {
        Class.forName(name).isInstance(t)
      } catch { case _: ClassNotFoundException => false }
    }

  /**
   * 执行带重试的操作。
   *
   * @param policy    重试策略
   * @param retryable 可重试异常判定（缺省使用白名单）
   * @param body      业务逻辑
   * @return 业务逻辑结果（成功时）
   * @throws IllegalStateException 重试耗尽仍失败时，抛出最后一次异常
   */
  def execute[A](policy: RetryConfig, retryable: Throwable => Boolean = defaultRetryable)(
      body: => A): A = {
    var attempt = 0
    var delay = policy.initialDelayMs
    var lastError: Throwable = null

    while (attempt < policy.maxAttempts) {
      attempt += 1
      try {
        return body
      } catch {
        case NonFatal(e) if attempt < policy.maxAttempts && retryable(e) =>
          lastError = e
          val actual = computeDelay(policy, delay)
          log.warn("第 {}/{} 次尝试失败（{}），{} ms 后重试。原因: {}",
            Integer.valueOf(attempt), Integer.valueOf(policy.maxAttempts),
            e.getClass.getSimpleName, java.lang.Long.valueOf(actual), e.getMessage)
          Thread.sleep(actual)
          delay = (delay * policy.multiplier).toLong
        case NonFatal(e) =>
          // 不可重试异常或已到最后一次：直接抛出
          throw e
      }
    }
    throw new IllegalStateException(
      s"重试 ${policy.maxAttempts} 次后仍失败，最后一次异常: ${lastError.getMessage}", lastError)
  }

  /** 计算本次实际等待时长（含抖动）。 */
  private def computeDelay(policy: RetryConfig, baseDelay: Long): Long = {
    val base =
      if (policy.policy == "exponential") baseDelay else policy.initialDelayMs
    if (policy.jitter) {
      val jitterFactor = 0.8 + Random.nextDouble() * 0.4 // ±20%
      (base * jitterFactor).toLong.max(0L)
    } else base
  }
}
