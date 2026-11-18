package com.sparketl.monitor.metrics

import java.util.concurrent.{ConcurrentHashMap, Executors, ScheduledExecutorService, ThreadFactory, TimeUnit}
import java.util.function.BiFunction

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import io.prometheus.client.{Collector, CollectorRegistry}
import io.prometheus.client.exporter.HTTPServer
import org.slf4j.LoggerFactory

import com.codahale.metrics.{Gauge, MetricRegistry}
import com.sparketl.core.metrics.EtlMetrics

/**
 * 基于 Dropwizard Metrics + Prometheus 的 [[EtlMetrics]] 生产实现。
 *
 * 能力：
 *  - counter / gauge / timer 全部落入内部 Dropwizard [[MetricRegistry]]；
 *  - 配置 `prometheusPort` 时启动内嵌 Prometheus HTTP Server（/metrics 端点），
 *    通过自定义 [[Collector]] 将 Dropwizard 指标映射为 Prometheus 指标：
 *      counter -> `name_total`（Counter 型）
 *      gauge   -> `name`（Gauge 型）
 *      timer   -> `name_count` 与 `name_total_ms`（Gauge 型）
 *    指标名中非 `[a-zA-Z0-9_]` 字符统一替换为 `_`（如 `etl.jobs.completed` -> `etl_jobs_completed_total`）；
 *  - `slf4j=true` 时启动单线程定时快照任务，每 60 秒经 SLF4J 输出指标快照（不依赖 metrics-slf4j）；
 *  - HTTP Server 启动失败时清理半初始化资源并抛出异常，
 *    由生命周期侧（resolveMetrics）捕获并回退 NoopMetrics；
 *  - [[close]] 停止 HTTP Server、注销 Collector 并关闭快照线程。
 *
 * @param prometheusPort Prometheus HTTP Server 端口（None 表示不启动）
 * @param slf4jEnabled   是否启用 SLF4J 定时快照输出
 * @param jobName        作业名称（用于日志标识）
 */
class PrometheusMetrics(prometheusPort: Option[Int], slf4jEnabled: Boolean, jobName: String)
    extends EtlMetrics {

  private val log = LoggerFactory.getLogger(classOf[PrometheusMetrics])

  private val registry = new MetricRegistry

  /** gauge 瞬时值（单一数据源，Prometheus 输出与快照均读取此处）。 */
  private val gaugeValues = new ConcurrentHashMap[String, java.lang.Double]()

  /** 已注册进 Dropwizard registry 的 gauge 名，保证只注册一次。 */
  private val registeredGauges = ConcurrentHashMap.newKeySet[String]()

  /** timer 累计毫秒总数（timer 的 _total_ms 输出）。 */
  private val timerTotals = new ConcurrentHashMap[String, java.lang.Double]()

  private var prometheusCollector: Collector = null
  private var prometheusServer: HTTPServer = null
  private var reporterExecutor: ScheduledExecutorService = null

  // ---------- 初始化 ----------
  if (slf4jEnabled) startSlf4jReporter()
  prometheusPort.foreach(startPrometheus)
  log.info(s"监控指标初始化完成: job=$jobName, prometheusPort=$prometheusPort, slf4jReporter=$slf4jEnabled")

  // ---------- EtlMetrics ----------

  /** 累加 Dropwizard Counter。 */
  override def incCounter(name: String, n: Long): Unit =
    registry.counter(name).inc(n)

  /** 更新 gauge 瞬时值（首次调用时注册进 Dropwizard registry，仅注册一次）。 */
  override def gauge(name: String, value: Double): Unit = {
    gaugeValues.put(name, java.lang.Double.valueOf(value))
    if (registeredGauges.add(name)) {
      try {
        registry.remove(name)
        registry.register(name, new Gauge[java.lang.Double] {
          override def getValue: java.lang.Double = gaugeValues.get(name)
        })
      } catch {
        case NonFatal(e) =>
          log.warn(s"gauge 注册 Dropwizard registry 失败（Prometheus 输出不受影响）: name=$name, error=${describe(e)}")
      }
    }
  }

  /** 记录一次耗时观测（毫秒），并累计总时长供 _total_ms 输出。 */
  override def timer(name: String, millis: Long): Unit = {
    registry.timer(name).update(millis, TimeUnit.MILLISECONDS)
    timerTotals.compute(name, new BiFunction[String, java.lang.Double, java.lang.Double] {
      override def apply(key: String, current: java.lang.Double): java.lang.Double =
        if (current == null) java.lang.Double.valueOf(millis.toDouble)
        else java.lang.Double.valueOf(current.doubleValue() + millis)
    })
  }

  /** 释放全部资源：关闭快照线程、停止 HTTP Server、注销 Collector。 */
  override def close(): Unit = {
    if (reporterExecutor != null) {
      try reporterExecutor.shutdownNow() catch { case NonFatal(e) =>
        log.warn(s"关闭指标快照线程失败: ${describe(e)}")
      }
      reporterExecutor = null
    }
    stopPrometheus()
    log.info(s"监控指标资源已释放: job=$jobName")
  }

  // ---------- Prometheus ----------

  /** 启动 Prometheus Collector 与内嵌 HTTP Server；失败时清理后抛出。 */
  private def startPrometheus(port: Int): Unit = {
    try {
      prometheusCollector = new DropwizardCollector
      prometheusCollector.register()
      prometheusServer = new HTTPServer(port)
      log.info(s"Prometheus HTTP Server 已启动: job=$jobName, port=$port")
    } catch {
      case NonFatal(e) =>
        stopPrometheus()
        throw new IllegalStateException(
          s"Prometheus HTTP Server 启动失败: job=$jobName, port=$port, error=${describe(e)}", e)
    }
  }

  /** 停止 HTTP Server 并从默认 CollectorRegistry 注销 Collector（幂等）。 */
  private def stopPrometheus(): Unit = {
    if (prometheusServer != null) {
      try prometheusServer.stop() catch { case NonFatal(e) =>
        log.warn(s"停止 Prometheus HTTP Server 失败: ${describe(e)}")
      }
      prometheusServer = null
    }
    if (prometheusCollector != null) {
      try CollectorRegistry.defaultRegistry.unregister(prometheusCollector) catch { case NonFatal(e) =>
        log.warn(s"注销 Prometheus Collector 失败: ${describe(e)}")
      }
      prometheusCollector = null
    }
  }

  /** 将 Dropwizard registry 的指标映射为 Prometheus MetricFamilySamples。 */
  private class DropwizardCollector extends Collector {

    override def collect(): java.util.List[Collector.MetricFamilySamples] = {
      val families = new java.util.ArrayList[Collector.MetricFamilySamples]()

      registry.getCounters.asScala.foreach { case (name, counter) =>
        families.add(singleSampleFamily(name, "_total", Collector.Type.COUNTER, counter.getCount.toDouble))
      }

      gaugeValues.asScala.foreach { case (name, value) =>
        families.add(singleSampleFamily(name, "", Collector.Type.GAUGE, value.doubleValue()))
      }

      registry.getTimers.asScala.foreach { case (name, timer) =>
        val totalMs = Option(timerTotals.get(name)).map(_.doubleValue()).getOrElse(0d)
        families.add(singleSampleFamily(name, "_count", Collector.Type.GAUGE, timer.getCount.toDouble))
        families.add(singleSampleFamily(name, "_total_ms", Collector.Type.GAUGE, totalMs))
      }

      families
    }
  }

  /** 构造仅含单个样本的 MetricFamilySamples（名称规范化 + 可选后缀）。 */
  private def singleSampleFamily(
      baseName: String,
      suffix: String,
      typ: Collector.Type,
      value: Double): Collector.MetricFamilySamples = {
    val name = PrometheusMetrics.normalize(baseName) + suffix
    val sample = new Collector.MetricFamilySamples.Sample(
      name,
      new java.util.ArrayList[String](),
      new java.util.ArrayList[String](),
      value)
    new Collector.MetricFamilySamples(
      name, typ, s"sparketl metric: $baseName", java.util.Collections.singletonList(sample))
  }

  // ---------- SLF4J 快照 ----------

  /** 启动单线程定时快照任务（守护线程，每 60 秒一次）。 */
  private def startSlf4jReporter(): Unit = {
    reporterExecutor = Executors.newSingleThreadScheduledExecutor(new ThreadFactory {
      override def newThread(r: Runnable): Thread = {
        val t = new Thread(r, "sparketl-metrics-slf4j-reporter")
        t.setDaemon(true)
        t
      }
    })
    reporterExecutor.scheduleWithFixedDelay(
      new Runnable { override def run(): Unit = reportSnapshot() },
      PrometheusMetrics.ReportIntervalSeconds, PrometheusMetrics.ReportIntervalSeconds, TimeUnit.SECONDS)
  }

  /** 输出当前指标快照（counters / gauges / timers）。 */
  private def reportSnapshot(): Unit = {
    try {
      val counters = registry.getCounters.asScala
        .map { case (k, v) => s"$k=${v.getCount}" }.mkString(", ")
      val gauges = gaugeValues.asScala
        .map { case (k, v) => s"$k=$v" }.mkString(", ")
      val timers = registry.getTimers.asScala
        .map { case (k, v) =>
          s"$k(count=${v.getCount}, totalMs=${Option(timerTotals.get(k)).map(_.toString).getOrElse("0")})"
        }.mkString(", ")
      log.info(s"[指标快照] job=$jobName counters={$counters} gauges={$gauges} timers={$timers}")
    } catch {
      case NonFatal(e) => log.warn(s"输出指标快照失败: ${describe(e)}")
    }
  }

  private def describe(e: Throwable): String = s"${e.getClass.getName}: ${e.getMessage}"
}

/** [[PrometheusMetrics]] 常量与工具。 */
object PrometheusMetrics {

  /** SLF4J 快照输出间隔（秒）。 */
  final val ReportIntervalSeconds = 60L

  /** 指标名规范化：非 [a-zA-Z0-9_] 字符替换为下划线，满足 Prometheus 命名规范。 */
  def normalize(name: String): String =
    if (name == null) "unknown" else name.replaceAll("[^a-zA-Z0-9_]", "_")
}
