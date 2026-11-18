package com.sparketl.launch

import scala.util.control.NonFatal

import org.slf4j.LoggerFactory

import com.sparketl.core.config.{ConfigException, ConfigParser}
import com.sparketl.core.lifecycle.JobLifecycle

/**
 * SparkETL 命令行入口（dist 模块）。
 *
 * 用法：
 * {{{
 * spark-submit --class com.sparketl.launch.SparkEtlEntrypoint sparketl-assembly-1.0.0.jar \
 *   --config /path/to/job.yaml
 * }}}
 *
 * 退出码约定：
 *  - 0：作业成功；
 *  - 1：作业执行失败（配置非法 / 抽取转换加载失败 / 质量阻断）；
 *  - 2：命令行参数错误。
 *
 * 注意：整个框架内部禁止 System.exit，仅本入口类在流程结束后
 * 以退出码收尾，符合容器/调度器（YARN、Airflow、Azkaban）集成规范。
 */
object SparkEtlEntrypoint {

  private val log = LoggerFactory.getLogger(getClass)

  private val Usage =
    """SparkETL 用法:
      |  spark-submit --class com.sparketl.launch.SparkEtlEntrypoint <assembly.jar> --config <job.yaml>
      |
      |参数:
      |  --config <path>   作业 YAML 配置文件路径（必填）
      |
      |可选（通过 spark-submit --conf 透传 Spark 参数，作业 YAML spark 段优先级更高）""".stripMargin

  def main(args: Array[String]): Unit = {
    val exitCode = parseAndRun(args)
    sys.exit(exitCode)
  }

  /** 解析参数并执行作业，返回进程退出码（便于单元测试）。 */
  private[launch] def parseAndRun(args: Array[String]): Int = {
    val configPath = parseArgs(args)
    if (configPath.isEmpty) {
      log.error(Usage)
      return 2
    }

    try {
      val jobConfig = ConfigParser.parseFile(configPath.get)
      log.info(s"作业配置加载成功: ${jobConfig.name}, 输出目标数: ${jobConfig.outputs.size}")
      new JobLifecycle(jobConfig).run()
      0
    } catch {
      case e: ConfigException =>
        log.error(s"配置错误: ${e.getMessage}")
        1
      case NonFatal(e) =>
        log.error(s"作业执行失败: ${e.getMessage}", e)
        1
    }
  }

  /** 解析命令行参数：--config <path>。 */
  private[launch] def parseArgs(args: Array[String]): Option[String] = {
    var path: Option[String] = None
    var i = 0
    while (i < args.length) {
      if (args(i) == "--config" && i + 1 < args.length) {
        path = Some(args(i + 1))
        i += 2
      } else {
        i += 1
      }
    }
    path
  }
}
