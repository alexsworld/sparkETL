package com.sparketl.quality.alert

import java.io.OutputStream
import java.net.HttpURLConnection
import java.net.URL
import java.nio.charset.StandardCharsets

import scala.util.control.NonFatal

import org.slf4j.LoggerFactory

import com.sparketl.core.config.AlertConfig
import com.sparketl.core.spi.QualityCheckResult

/**
 * 质量告警分发器：表级规则未通过时向配置的告警通道发送通知。
 *
 * 支持通道（YAML alerts[].type）：
 *  - `log`：以 error 级别输出聚合告警内容
 *  - `webhook`：POST JSON 到指定 url（connect/read 超时 5 秒）
 *
 * 告警发送失败不应阻断主流程：webhook 异常仅记录 warn 日志后吞掉。
 */
object AlertDispatcher {

  private val log = LoggerFactory.getLogger(getClass)

  private val ConnectTimeoutMs = 5000
  private val ReadTimeoutMs = 5000

  /**
   * 分发告警。无失败项时直接返回，不做任何发送。
   *
   * @param alerts     告警通道配置列表
   * @param jobSubject 作业主题（告警体中的 job 字段）
   * @param failures   未通过的校验结果
   */
  def dispatch(alerts: Seq[AlertConfig], jobSubject: String, failures: Seq[QualityCheckResult]): Unit = {
    if (failures.isEmpty) return
    alerts.foreach {
      case AlertConfig(t, _) if t.equalsIgnoreCase("log") =>
        val detail = failures.map(f => s"${f.ruleName}(metric=${f.metric}): ${f.message}").mkString("; ")
        log.error(s"质量告警[$jobSubject]: ${failures.size} 条规则未通过 - $detail")
      case AlertConfig(t, Some(url)) if t.equalsIgnoreCase("webhook") =>
        postWebhook(url, buildBody(jobSubject, failures))
      case AlertConfig(t, None) if t.equalsIgnoreCase("webhook") =>
        log.warn(s"webhook 告警缺少 url 配置，跳过发送")
      case other =>
        log.warn(s"未知的告警通道类型 '${other.dsType}'，跳过发送")
    }
  }

  /** 构造告警 JSON 体：{"job":"...","level":"QUALITY","failures":[{"rule":"...","metric":1,"message":"..."}]}。 */
  private def buildBody(job: String, failures: Seq[QualityCheckResult]): String = {
    val items = failures.map { f =>
      s"""{"rule":"${escapeJson(f.ruleName)}","metric":${f.metric},"message":"${escapeJson(f.message)}"}"""
    }.mkString(",")
    s"""{"job":"${escapeJson(job)}","level":"QUALITY","failures":[$items]}"""
  }

  /** JSON 字符串转义：处理引号、反斜杠与控制字符。 */
  private def escapeJson(raw: String): String =
    raw.flatMap {
      case '"'  => "\\\""
      case '\\' => "\\\\"
      case '\n' => "\\n"
      case '\r' => "\\r"
      case '\t' => "\\t"
      case c    => c.toString
    }

  /** POST JSON 到 webhook 端点，失败仅记录 warn 日志。 */
  private def postWebhook(url: String, body: String): Unit = {
    try {
      val conn = new URL(url).openConnection().asInstanceOf[HttpURLConnection]
      try {
        conn.setRequestMethod("POST")
        conn.setConnectTimeout(ConnectTimeoutMs)
        conn.setReadTimeout(ReadTimeoutMs)
        conn.setDoOutput(true)
        conn.setRequestProperty("Content-Type", "application/json")
        conn.connect()
        val os: OutputStream = conn.getOutputStream
        try os.write(body.getBytes(StandardCharsets.UTF_8))
        finally os.close()
        val code = conn.getResponseCode
        if (code >= 200 && code < 300) {
          log.info(s"webhook 告警发送成功: url=$url")
        } else {
          log.warn(s"webhook 告警返回非 2xx 状态码: code=$code, url=$url")
        }
      } finally conn.disconnect()
    } catch {
      case NonFatal(e) => log.warn(s"webhook 告警发送失败（忽略）: ${e.getMessage}")
    }
  }
}
