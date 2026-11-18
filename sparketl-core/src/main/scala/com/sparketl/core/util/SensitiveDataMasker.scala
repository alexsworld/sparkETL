package com.sparketl.core.util

import scala.collection.JavaConverters._

import org.apache.spark.SparkConf

/**
 * 敏感信息脱敏工具：日志与运行记录中禁止出现明文密码 / 密钥。
 *
 * 规则：键名包含 password / secret / token / credential / accessKey /
 * access-key（不区分大小写）时，值仅保留前 2 个字符，其余以 `***` 替代。
 */
object SensitiveDataMasker {

  private val SensitiveKeys = Seq("password", "secret", "token", "credential", "accesskey")

  private def isSensitive(key: String): Boolean = {
    val lower = key.toLowerCase
    SensitiveKeys.exists(lower.contains)
  }

  /** 对单个键值对脱敏。 */
  def mask(key: String, value: String): String =
    if (isSensitive(key)) {
      if (value == null || value.length <= 2) "***"
      else value.substring(0, 2) + "***"
    } else value

  /** 对键值映射脱敏。 */
  def maskAll(options: Map[String, String]): Map[String, String] =
    options.map { case (k, v) => k -> mask(k, v) }

  /** 对 SparkConf 脱敏后输出可安全打印的字符串。 */
  def maskConf(conf: SparkConf): String =
    conf.getAll.map { case (k, v) => s"$k=${mask(k, v)}" }.mkString(", ")

  /** 对系统属性形式的连接选项（如 Kafka options）脱敏。 */
  def maskJavaMap(options: java.util.Map[String, String]): String =
    options.asScala.map { case (k, v) => s"$k=${mask(k, v)}" }.mkString(", ")
}
