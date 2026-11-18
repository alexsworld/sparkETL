package com.sparketl.core.config

/**
 * 配置解析 / 校验异常。
 *
 * 在作业提交到 Spark 集群之前即抛出，用于快速失败（fail-fast），
 * 错误信息必须能够直接定位到配置文件中的具体问题。
 *
 * @param message 错误描述
 * @param cause   根因（可选）
 */
class ConfigException(message: String, cause: Throwable = null)
  extends RuntimeException(message, cause)
