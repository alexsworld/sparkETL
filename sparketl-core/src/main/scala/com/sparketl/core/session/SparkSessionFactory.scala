package com.sparketl.core.session

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import com.sparketl.core.config.JobConfig
import com.sparketl.core.util.SensitiveDataMasker

/**
 * SparkSession 工厂：封装 Spark 3.5.x 生产级默认优化配置。
 *
 * 默认开启：
 *  - AQE 自适应查询执行（含自动分区合并、自动倾斜join优化）；
 *  - 动态分区裁剪（DPE，Spark 3.x 默认开启，此处显式声明）；
 *  - Parquet / ORC 向量化读取；
 *  - KryoSerializer 序列化框架；
 *  - 动态分区覆盖模式（overwrite 仅覆盖相关分区）。
 *
 * 配置优先级（高 -> 低）：
 *  1. 作业 YAML 的 spark 段（含 spark.master / sparketl.hiveSupport）；
 *  2. 命令行透传配置（spark-submit --conf 已在 JVM 属性生效者除外）；
 *  3. 框架默认值。
 *
 * Hive 支持：`sparketl.hiveSupport` 取 true | false | auto（缺省 auto，
 * 即 input / outputs 含 hive 类型连接器时自动启用）。
 */
object SparkSessionFactory {

  private val log = LoggerFactory.getLogger(getClass)

  /** 框架默认 SparkConf（生产级优化基线）。 */
  val DefaultConf: Map[String, String] = Map(
    "spark.sql.adaptive.enabled" -> "true",
    "spark.sql.adaptive.coalescePartitions.enabled" -> "true",
    "spark.sql.adaptive.skewJoin.enabled" -> "true",
    "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.parquet.enableVectorizedReader" -> "true",
    "spark.sql.orc.enableVectorizedReader" -> "true",
    "spark.sql.sources.partitionOverwriteMode" -> "dynamic"
  )

  /** Hive 支持开关的配置键（放在作业 YAML 的 spark 段中）。 */
  val HiveSupportKey = "sparketl.hiveSupport"

  /**
   * 创建 SparkSession。
   *
   * @param job     作业配置
   * @param cliConf 命令行透传的 Spark 配置（优先级低于作业 YAML）
   * @return 已配置 AQE / 向量化等优化的 SparkSession
   */
  def create(job: JobConfig, cliConf: Map[String, String] = Map.empty): SparkSession = {
    val jobConf = job.spark.getOrElse(Map.empty)

    // 优先级：作业 YAML > 命令行 > 默认
    val effective = DefaultConf ++ cliConf ++ jobConf

    val master = effective.getOrElse("spark.master", "local[*]")
    val builder = SparkSession.builder()
      .master(master)
      .appName(job.name)

    effective.foreach { case (k, v) => builder.config(k, v) }

    if (needHiveSupport(job, jobConf)) {
      builder.enableHiveSupport()
    }

    val session = builder.getOrCreate()
    val isHive = session.conf.getOption("spark.sql.catalogImplementation").contains("hive")
    log.info(s"SparkSession 创建完成: master=$master, hiveSupport=$isHive, " +
      SensitiveDataMasker.maskAll(effective.filterKeys(_.startsWith("spark.")).toMap))
    session
  }

  /** 判定是否需要启用 Hive 支持（true | false | auto）。 */
  private def needHiveSupport(job: JobConfig, jobConf: Map[String, String]): Boolean = {
    val mode = jobConf.getOrElse(HiveSupportKey, "auto").toLowerCase
    mode match {
      case "true" => true
      case "false" => false
      case "auto" =>
        job.input.dsType.equalsIgnoreCase("hive") ||
        job.outputs.exists(_.dsType.equalsIgnoreCase("hive"))
      case other =>
        log.warn(s"非法的 $HiveSupportKey 值 '$other'，回退为 auto")
        job.input.dsType.equalsIgnoreCase("hive") ||
        job.outputs.exists(_.dsType.equalsIgnoreCase("hive"))
    }
  }
}
