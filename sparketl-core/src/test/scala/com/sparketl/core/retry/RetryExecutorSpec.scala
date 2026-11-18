package com.sparketl.core.retry

import java.sql.SQLException

import org.scalatest.funsuite.AnyFunSuite

import com.sparketl.core.config.RetryConfig

/** 重试执行器单元测试：失败 N 次后成功 / 重试耗尽 / 不可重试异常直接抛出。 */
class RetryExecutorSpec extends AnyFunSuite {

  test("前两次失败第三次成功") {
    val policy = RetryConfig(maxAttempts = 3, policy = "fixed", initialDelayMs = 1, jitter = false)
    var attempts = 0
    val result = RetryExecutor.execute(policy) {
      attempts += 1
      if (attempts < 3) throw new SQLException("模拟连接失败") else "ok"
    }
    assert(result == "ok")
    assert(attempts == 3)
  }

  test("重试耗尽后抛出最后一次异常") {
    val policy = RetryConfig(maxAttempts = 2, policy = "fixed", initialDelayMs = 1, jitter = false)
    var attempts = 0
    val ex = intercept[SQLException] {
      RetryExecutor.execute(policy) {
        attempts += 1
        throw new SQLException(s"始终失败 #$attempts")
      }
    }
    assert(ex.getMessage.contains("始终失败 #2"))
    assert(attempts == 2)
  }

  test("不可重试异常不做重试直接抛出") {
    val policy = RetryConfig(maxAttempts = 3, policy = "fixed", initialDelayMs = 1)
    var attempts = 0
    assertThrows[IllegalArgumentException] {
      RetryExecutor.execute(policy) {
        attempts += 1
        throw new IllegalArgumentException("参数错误不可重试")
      }
    }
    assert(attempts == 1)
  }

  test("exponential 策略按倍数退避（不抛异常验证通过即可）") {
    val policy = RetryConfig(maxAttempts = 2, policy = "exponential",
      initialDelayMs = 1, multiplier = 3.0, jitter = false)
    var attempts = 0
    val result = RetryExecutor.execute(policy) {
      attempts += 1
      if (attempts < 2) throw new java.io.IOException("瞬时 IO 故障") else 42
    }
    assert(result == 42)
    assert(attempts == 2)
  }
}
