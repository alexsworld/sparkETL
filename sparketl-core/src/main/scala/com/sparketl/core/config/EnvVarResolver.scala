package com.sparketl.core.config

import java.util.regex.{Matcher, Pattern}

import scala.collection.JavaConverters._

/**
 * 环境变量占位符解析器。
 *
 * 支持形如 `${env:VAR}` 或 `${env:VAR:defaultValue}` 的占位符，
 * 递归解析 YAML 配置树中的所有字符串节点，避免密码、AccessKey 等
 * 敏感信息以明文形式出现在配置文件中。
 *
 * 解析规则：
 *  - 环境变量存在：替换为环境变量值；
 *  - 环境变量不存在但提供了默认值：替换为默认值；
 *  - 环境变量不存在且无默认值：抛出 [[ConfigException]]。
 */
private[config] object EnvVarResolver {

  /** 占位符模式：`${env:VAR}` 或 `${env:VAR:default}`。 */
  private val Placeholder: Pattern =
    Pattern.compile("""\$\{env:([A-Za-z0-9_.\-]+)(?::((?s).*))?\}""")

  /** 递归解析任意 YAML 节点（Map / List / String / 标量）。 */
  def resolve(node: Any): Any = node match {
    case s: String            => resolveString(s)
    case m: java.util.Map[_, _] =>
      m.asScala.map { case (k, v) => k -> resolve(v) }.toMap.asJava
    case l: java.util.List[_] =>
      l.asScala.map(resolve).asJava
    case other => other
  }

  /** 将字符串中的所有占位符替换为环境变量值（或默认值）。 */
  private def resolveString(value: String): String = {
    val matcher = Placeholder.matcher(value)
    val sb = new StringBuffer
    while (matcher.find()) {
      val variable = matcher.group(1)
      val default = matcher.group(2)
      val resolved = Option(System.getenv(variable))
        .orElse(Option(default))
        .getOrElse(throw new ConfigException(
          "环境变量未设置且未提供默认值: ${env:" + variable + "}"))
      matcher.appendReplacement(sb, Matcher.quoteReplacement(resolved))
    }
    matcher.appendTail(sb)
    sb.toString
  }
}
