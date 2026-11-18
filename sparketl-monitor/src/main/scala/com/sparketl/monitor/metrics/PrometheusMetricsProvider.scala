package com.sparketl.monitor.metrics

import com.sparketl.core.config.MetricsConfig
import com.sparketl.core.metrics.{EtlMetrics, MetricsProvider}

/**
 * Prometheus 指标实现的 SPI 工厂。
 *
 * 通过 Java ServiceLoader 注册（见 META-INF/services），
 * 生命周期根据 YAML `monitor.metrics` 段经 [[MetricsProvider]] 创建指标实现。
 */
class PrometheusMetricsProvider extends MetricsProvider {

  /** 根据指标配置创建 [[PrometheusMetrics]]：slf4j 未配置时默认关闭快照输出。 */
  override def create(config: MetricsConfig, jobName: String): EtlMetrics =
    new PrometheusMetrics(config.prometheusPort, config.slf4j.getOrElse(false), jobName)
}
