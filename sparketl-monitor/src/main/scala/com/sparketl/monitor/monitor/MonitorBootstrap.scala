package com.sparketl.monitor.monitor

import org.apache.spark.sql.SparkSession

import com.sparketl.core.metrics.EtlMetrics
import com.sparketl.monitor.listener.MetricsSparkListener

/**
 * 监控模块引导入口：提供把监控埋点挂载到 SparkSession 的辅助方法。
 *
 * 生命周期已通过 onSparkReady 回调集成监控；独立使用（测试 / 示例 / 文档）时
 * 可直接调用本工具完成 Listener 挂载。
 */
object MonitorBootstrap {

  /** 向 SparkSession 注册 [[MetricsSparkListener]]，采集 Stage / Task / Job 指标。 */
  def attachListener(spark: SparkSession, metrics: EtlMetrics): Unit =
    spark.sparkContext.addSparkListener(new MetricsSparkListener(metrics))
}
