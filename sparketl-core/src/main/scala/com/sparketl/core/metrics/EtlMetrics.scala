package com.sparketl.core.metrics

import com.sparketl.core.config.MetricsConfig

/**
 * ETL 指标采集接口。
 *
 * 框架在各生命周期阶段上报关键指标（行数、耗时、失败数等）；
 * 默认实现为 [[NoopMetrics]]，生产实现（Dropwizard + Prometheus）
 * 位于 sparketl-monitor 模块，通过 [[MetricsProvider]] 经 ServiceLoader 创建。
 */
trait EtlMetrics {

  /** 累加计数器。 */
  def incCounter(name: String, n: Long = 1L): Unit

  /** 设置瞬时值指标。 */
  def gauge(name: String, value: Double): Unit

  /** 记录一次耗时观测（毫秒）。 */
  def timer(name: String, millis: Long): Unit

  /** 释放资源（如 Prometheus HTTP Server）；默认空实现。 */
  def close(): Unit = ()
}

/** 空实现：未启用监控时使用，零开销。 */
object NoopMetrics extends EtlMetrics {
  override def incCounter(name: String, n: Long): Unit = ()
  override def gauge(name: String, value: Double): Unit = ()
  override def timer(name: String, millis: Long): Unit = ()
}

/**
 * 指标实现工厂 SPI。
 *
 * 实现类通过 Java ServiceLoader 注册；未注册或未配置 metrics 时，
 * 生命周期自动回退到 [[NoopMetrics]]。
 */
trait MetricsProvider {

  /** 根据指标配置创建指标实现。 */
  def create(config: MetricsConfig, jobName: String): EtlMetrics
}
