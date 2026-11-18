package com.sparketl.monitor.metrics

import java.net.{HttpURLConnection, URL}

import scala.io.Source
import scala.util.control.NonFatal

import org.scalatest.funsuite.AnyFunSuite

/**
 * [[PrometheusMetrics]] 测试：启动内嵌 HTTP Server 后验证 /metrics 端点输出。
 */
class PrometheusMetricsSpec extends AnyFunSuite {

  private val BasePort = 19876

  test("incCounter / gauge / timer 指标经 Prometheus /metrics 端点暴露") {
    val started = tryStart()
    assume(started.isDefined, "无法绑定 Prometheus 测试端口，跳过本用例")
    val port = started.get
    val metrics = new PrometheusMetrics(Some(port), slf4jEnabled = false, jobName = "test")
    try {
      metrics.incCounter("etl.jobs.completed", 3)
      metrics.gauge("etl.queue.size", 7.0)
      metrics.timer("etl.stage.duration", 123)

      val body = httpGet(s"http://127.0.0.1:$port/metrics")

      assert(body.contains("etl_jobs_completed_total 3"), s"counter 未暴露: $body")
      assert(
        """(?m)^etl_queue_size 7(\.0)?$""".r.findFirstIn(body).isDefined,
        s"gauge 未暴露: $body")
      assert(body.contains("etl_stage_duration_count 1"), s"timer count 未暴露: $body")
      assert(
        """(?m)^etl_stage_duration_total_ms 123(\.0)?$""".r.findFirstIn(body).isDefined,
        s"timer total 未暴露: $body")
    } finally {
      metrics.close()
    }
  }

  /** 依次尝试 BasePort 起的连续端口，返回绑定成功的端口（全失败返回 None）。 */
  private def tryStart(): Option[Int] = {
    var port = BasePort
    var result: Option[Int] = None
    while (result.isEmpty && port < BasePort + 5) {
      try {
        val m = new PrometheusMetrics(Some(port), slf4jEnabled = false, jobName = "probe")
        m.close()
        result = Some(port)
      } catch {
        case NonFatal(_) => port += 1
      }
    }
    result
  }

  private def httpGet(urlStr: String): String = {
    val conn = new URL(urlStr).openConnection().asInstanceOf[HttpURLConnection]
    try {
      conn.setRequestMethod("GET")
      conn.setConnectTimeout(5000)
      conn.setReadTimeout(5000)
      val in = conn.getInputStream
      try Source.fromInputStream(in, "UTF-8").mkString
      finally in.close()
    } finally conn.disconnect()
  }
}
