package com.sparketl.monitor.listener

import org.apache.spark.Success
import org.apache.spark.scheduler.{
  SparkListener,
  SparkListenerJobEnd,
  SparkListenerStageCompleted,
  SparkListenerTaskEnd
}
import org.slf4j.LoggerFactory

import com.sparketl.core.metrics.EtlMetrics

/**
 * Spark 指标采集监听器：把 Spark 调度事件转写为 [[EtlMetrics]] 指标。
 *
 * 采集项：
 *  - Stage 完成：耗时 `spark.stage.duration.millis`（completionTime - submissionTime），
 *    计数 `spark.stages.completed`；
 *  - Task 结束：非 Success 结果计数 `spark.tasks.failed`；
 *  - Job 完成：计数 `spark.jobs.completed`。
 *
 * 通过 [[com.sparketl.monitor.monitor.MonitorBootstrap.attachListener]] 挂载。
 *
 * @param metrics 指标上报目标
 */
class MetricsSparkListener(metrics: EtlMetrics) extends SparkListener {

  private val log = LoggerFactory.getLogger(classOf[MetricsSparkListener])

  /** Stage 完成时上报耗时与完成计数。 */
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val info = stageCompleted.stageInfo
    val duration = info.completionTime.getOrElse(0L) - info.submissionTime.getOrElse(0L)
    metrics.timer("spark.stage.duration.millis", math.max(duration, 0L))
    metrics.incCounter("spark.stages.completed")
  }

  /** Task 结束时统计失败结果（非 Success 的 reason）。 */
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    if (taskEnd.reason != Success) {
      metrics.incCounter("spark.tasks.failed")
    }
  }

  /** Job 完成时上报完成计数。 */
  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    metrics.incCounter("spark.jobs.completed")
    log.debug(s"Spark Job 完成: jobId=${jobEnd.jobId}")
  }
}
