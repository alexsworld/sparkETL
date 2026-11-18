package com.sparketl.core.config

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import org.slf4j.LoggerFactory
import org.yaml.snakeyaml.Yaml

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

/**
 * 作业 YAML 配置解析器，将 YAML 文本解析为强类型 [[JobConfig]]。
 *
 * 解析流程：
 *  1. SnakeYAML 加载为 Java Map 结构；
 *  2. [[EnvVarResolver]] 递归解析 `${env:VAR}` 占位符；
 *  3. 逐层映射为 case class，类型不合法时抛出 [[ConfigException]]。
 */
object ConfigParser {

  private val log = LoggerFactory.getLogger(getClass)

  /** 从文件路径解析配置（UTF-8 编码）。 */
  def parseFile(path: String): JobConfig = {
    val file = Paths.get(path)
    if (!Files.isRegularFile(file)) {
      throw new ConfigException(s"配置文件不存在或不是普通文件: $path")
    }
    parse(new String(Files.readAllBytes(file), StandardCharsets.UTF_8))
  }

  /** 从 YAML 字符串解析配置。 */
  def parse(content: String): JobConfig =
    try {
      parseNode(new Yaml().load(content))
    } catch {
      case e: ConfigException => throw e
      case NonFatal(e)        => throw new ConfigException(s"YAML 解析失败: ${e.getMessage}", e)
    }

  // ------------------------------------------------------------------
  //  树形结构 -> case class
  // ------------------------------------------------------------------

  private[config] def parseNode(node: Any): JobConfig = {
    val root = toMap(EnvVarResolver.resolve(node), "<root>")

    val name = requiredStr(root, "name")
    val batchTime = optionalStr(root, "batchTime")
    val spark = optionalStrMap(root, "spark")

    val inputNode = requiredMap(root, "input")
    val input = InputConfig(
      dsType = requiredStr(inputNode, "type", "input"),
      options = optionalStrMap(inputNode, "options").getOrElse(Map.empty)
    )

    val outputs = requiredSeq(root, "outputs").zipWithIndex.map { case (o, idx) =>
      val m = toMap(o, s"outputs[$idx]")
      OutputConfig(
        dsType = requiredStr(m, "type", s"outputs[$idx]"),
        mode = optionalStr(m, "mode").getOrElse("append"),
        options = optionalStrMap(m, "options").getOrElse(Map.empty)
      )
    }

    val transforms = optionalSeq(root, "transforms").getOrElse(Seq.empty).zipWithIndex.map {
      case (t, idx) =>
        val m = toMap(t, s"transforms[$idx]")
        TransformConfig(
          kind = requiredStr(m, "kind", s"transforms[$idx]"),
          name = optionalStr(m, "name"),
          params = optionalStrMap(m, "params").getOrElse(Map.empty)
        )
    }

    val quality = optionalMap(root, "quality").map(buildQuality)
    val retry = optionalMap(root, "retry").map(buildRetry)
    val monitor = optionalMap(root, "monitor").map(buildMonitor)

    JobConfig(name, batchTime, spark, input, outputs, transforms, quality, retry, monitor)
  }

  private def buildQuality(node: Map[String, Any]): QualityConfig = {
    val rowRules = optionalSeq(node, "rowRules").getOrElse(Seq.empty).zipWithIndex.map {
      case (r, idx) =>
        val m = toMap(r, s"quality.rowRules[$idx]")
        RowRule(
          name = requiredStr(m, "name", s"quality.rowRules[$idx]"),
          expression = requiredStr(m, "expression", s"quality.rowRules[$idx]")
        )
    }
    val tableRules = optionalSeq(node, "tableRules").getOrElse(Seq.empty).zipWithIndex.map {
      case (r, idx) =>
        val m = toMap(r, s"quality.tableRules[$idx]")
        TableRule(
          name = requiredStr(m, "name", s"quality.tableRules[$idx]"),
          kind = requiredStr(m, "kind", s"quality.tableRules[$idx]"),
          params = optionalStrMap(m, "params").getOrElse(Map.empty)
        )
    }
    val quarantine = optionalMap(node, "quarantine").map { q =>
      QuarantineConfig(
        dsType = requiredStr(q, "type", "quality.quarantine"),
        path = optionalStr(q, "path"),
        options = optionalStrMap(q, "options").getOrElse(Map.empty)
      )
    }
    val alerts = optionalSeq(node, "alerts").getOrElse(Seq.empty).zipWithIndex.map {
      case (a, idx) =>
        val m = toMap(a, s"quality.alerts[$idx]")
        AlertConfig(
          dsType = requiredStr(m, "type", s"quality.alerts[$idx]"),
          url = optionalStr(m, "url")
        )
    }
    QualityConfig(
      rowRules = rowRules,
      tableRules = tableRules,
      onFailure = optionalStr(node, "onFailure").getOrElse("WARN"),
      quarantine = quarantine,
      alerts = alerts
    )
  }

  private def buildRetry(node: Map[String, Any]): RetryConfig =
    RetryConfig(
      maxAttempts = optionalInt(node, "maxAttempts").getOrElse(3),
      policy = optionalStr(node, "policy").getOrElse("fixed"),
      initialDelayMs = optionalLong(node, "initialDelayMs").getOrElse(1000L),
      multiplier = optionalDouble(node, "multiplier").getOrElse(2.0),
      jitter = optionalBool(node, "jitter").getOrElse(true)
    )

  private def buildMonitor(node: Map[String, Any]): MonitorConfig = {
    val runStore = optionalMap(node, "runStore").map { r =>
      RunStoreConfig(
        url = requiredStr(r, "url", "monitor.runStore"),
        user = optionalStr(r, "user"),
        password = optionalStr(r, "password"),
        table = optionalStr(r, "table")
      )
    }
    val metrics = optionalMap(node, "metrics").map { m =>
      MetricsConfig(
        prometheusPort = optionalInt(m, "prometheusPort"),
        slf4j = optionalBool(m, "slf4j")
      )
    }
    MonitorConfig(runStore = runStore, metrics = metrics)
  }

  // ------------------------------------------------------------------
  //  节点取值工具（含类型强校验）
  // ------------------------------------------------------------------

  private[config] def toMap(node: Any, ctx: String): Map[String, Any] = node match {
    case m: java.util.Map[_, _] =>
      m.asScala.toMap.map { case (k, v) => String.valueOf(k) -> v }
    case null => throw new ConfigException(s"$ctx: 期望映射结构，实际为空")
    case other =>
      throw new ConfigException(s"$ctx: 期望映射结构，实际为 ${other.getClass.getSimpleName}")
  }

  private def asString(v: Any): String = v match {
    case null             => null
    case s: String        => s
    case n: Number        => String.valueOf(n)
    case b: Boolean       => String.valueOf(b)
    case other            => String.valueOf(other)
  }

  private def asInt(v: Any, key: String): Int = v match {
    case n: Number => n.intValue()
    case s: String =>
      try s.trim.toInt
      catch { case _: NumberFormatException => throw new ConfigException(s"$key: 期望整数，实际为 '$s'") }
    case null => throw new ConfigException(s"$key: 期望整数，实际为空")
    case other => throw new ConfigException(s"$key: 期望整数，实际为 ${other.getClass.getSimpleName}")
  }

  private def asLong(v: Any, key: String): Long = v match {
    case n: Number => n.longValue()
    case s: String =>
      try s.trim.toLong
      catch { case _: NumberFormatException => throw new ConfigException(s"$key: 期望长整数，实际为 '$s'") }
    case null => throw new ConfigException(s"$key: 期望长整数，实际为空")
    case other => throw new ConfigException(s"$key: 期望长整数，实际为 ${other.getClass.getSimpleName}")
  }

  private def asDouble(v: Any, key: String): Double = v match {
    case n: Number => n.doubleValue()
    case s: String =>
      try s.trim.toDouble
      catch { case _: NumberFormatException => throw new ConfigException(s"$key: 期望数值，实际为 '$s'") }
    case null => throw new ConfigException(s"$key: 期望数值，实际为空")
    case other => throw new ConfigException(s"$key: 期望数值，实际为 ${other.getClass.getSimpleName}")
  }

  private def asBoolean(v: Any, key: String): Boolean = v match {
    case b: Boolean => b
    case s: String =>
      try s.trim.toBoolean
      catch { case _: IllegalArgumentException => throw new ConfigException(s"$key: 期望布尔值，实际为 '$s'") }
    case null => throw new ConfigException(s"$key: 期望布尔值，实际为空")
    case other => throw new ConfigException(s"$key: 期望布尔值，实际为 ${other.getClass.getSimpleName}")
  }

  private def requiredStr(m: Map[String, Any], key: String, ctx: String = "root"): String =
    m.get(key).map(asString).map(_.trim).filter(_.nonEmpty).getOrElse(
      throw new ConfigException(s"$ctx.$key: 必填项缺失或为空"))

  private def optionalStr(m: Map[String, Any], key: String): Option[String] =
    m.get(key).map(asString).map(_.trim).filter(_.nonEmpty)

  private def optionalInt(m: Map[String, Any], key: String): Option[Int] =
    m.get(key).map(v => asInt(v, key))

  private def optionalLong(m: Map[String, Any], key: String): Option[Long] =
    m.get(key).map(v => asLong(v, key))

  private def optionalDouble(m: Map[String, Any], key: String): Option[Double] =
    m.get(key).map(v => asDouble(v, key))

  private def optionalBool(m: Map[String, Any], key: String): Option[Boolean] =
    m.get(key).map(v => asBoolean(v, key))

  private def requiredMap(m: Map[String, Any], key: String): Map[String, Any] =
    optionalMap(m, key).getOrElse(throw new ConfigException(s"root.$key: 必填项缺失或不是映射结构"))

  private def optionalMap(m: Map[String, Any], key: String): Option[Map[String, Any]] =
    m.get(key).filter(_ != null).map(v => toMap(v, key))

  private def requiredSeq(m: Map[String, Any], key: String): Seq[Any] =
    optionalSeq(m, key).getOrElse(throw new ConfigException(s"root.$key: 必填项缺失或不是列表结构"))

  private def optionalSeq(m: Map[String, Any], key: String): Option[Seq[Any]] =
    m.get(key).filter(_ != null).map {
      case l: java.util.List[_] => l.asScala.toSeq
      case other => throw new ConfigException(s"root.$key: 期望列表结构，实际为 ${other.getClass.getSimpleName}")
    }

  /** 选项映射：值统一转为字符串，供连接器透传。 */
  private[config] def optionalStrMap(m: Map[String, Any], key: String): Option[Map[String, String]] =
    m.get(key).filter(_ != null).map { v =>
      toMap(v, key).map { case (k, value) => k -> Option(asString(value)).getOrElse("") }
    }
}
