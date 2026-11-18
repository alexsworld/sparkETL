package com.sparketl.core.lifecycle

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.ServiceLoader

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import com.sparketl.core.config.{JobConfig, MetricsConfig, QualityConfig}
import com.sparketl.core.exception.{EtlException, JobPhase, QualityBlockException}
import com.sparketl.core.metrics.{EtlMetrics, MetricsProvider, NoopMetrics}
import com.sparketl.core.registry.Registry
import com.sparketl.core.retry.RetryExecutor
import com.sparketl.core.session.SparkSessionFactory
import com.sparketl.core.spi.{JobRunStoreProvider, JobStatus, QualityChecker, QualityOutcome, RunStoreConfigHolder}
import com.sparketl.core.transform.TransformResolver

/**
 * ETL 作业生命周期管理器：标准化封装一次作业运行的完整流程。
 *
 * 阶段编排：
 * {{{
 * VALIDATE -> INIT -> EXTRACT -> TRANSFORM -> QUALITY -> LOAD -> FINALIZE -> CLEANUP
 * }}}
 *
 * 职责：
 *  - 提交集群前完成配置快速失败校验（VALIDATE）；
 *  - 初始化 SparkSession 并挂载监控埋点（INIT）；
 *  - 重试包裹的数据抽取与行数统计（EXTRACT）；
 *  - 顺序执行转换链（TRANSFORM）；
 *  - 行级分流 + 表级校验 + 隔离区 + 告警（QUALITY，FAIL 策略阻断）；
 *  - 多目标输出复用同一份合规数据（LOAD）；
 *  - 运行记录落库、指标上报（FINALIZE）；
 *  - SparkSession 关闭与资源清理（CLEANUP，finally 保证）。
 *
 * 框架内部禁止 System.exit；失败以异常形式抛出，由调用方（CLI 容器）
 * 决定进程退出码。
 *
 * @param config     作业配置（已完成解析）
 * @param cliConf    命令行透传的 Spark 配置（优先级低于作业 YAML）
 */
class JobLifecycle(config: JobConfig, cliConf: Map[String, String] = Map.empty) {

  private val log = LoggerFactory.getLogger(classOf[JobLifecycle])

  private val RunIdFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")

  /** 供监控模块挂载 SparkListener 等扩展的回调（onSparkReady）。 */
  private var sparkReadyHook: Option[SparkSession => Unit] = None

  /** 注册 SparkSession 就绪回调（监控模块用于挂载 Listener）。 */
  def onSparkReady(hook: SparkSession => Unit): Unit = {
    sparkReadyHook = Some(hook)
  }

  /**
   * 执行完整作业流程。
   *
   * @throws com.sparketl.core.config.ConfigException 配置非法
   * @throws QualityBlockException                    质量规则阻断
   * @throws EtlException                             其他作业执行失败（携带阶段）
   */
  def run(): Unit = {
    // ---------- VALIDATE ----------
    enterPhase(JobPhase.Validate)
    com.sparketl.core.config.ConfigValidator.validate(config)

    val runId = s"${config.name}-${LocalDateTime.now().format(RunIdFormatter)}-${uuidSuffix()}"
    val batchTime = config.batchTime.getOrElse(
      LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))

    val metrics = resolveMetrics()
    val runStore = resolveRunStore()
    val ctx = new JobContext(config, runId, batchTime, metrics)
    ctx.runStore = runStore

    var spark: SparkSession = null
    try {
      runStore.foreach(_.start(ctx.record(JobStatus.Running)))

      // ---------- INIT ----------
      ctx.enterPhase(JobPhase.Init)
      spark = SparkSessionFactory.create(config, cliConf)
      sparkReadyHook.foreach(_.apply(spark))

      // ---------- EXTRACT（重试包裹） ----------
      ctx.enterPhase(JobPhase.Extract)
      val reader = Registry.provider(config.input.dsType).createReader(config.input.options)
      val retryPolicy = config.retry.getOrElse(
        com.sparketl.core.config.RetryConfig())
      val raw = RetryExecutor.execute(retryPolicy)(reader.read(spark))
      raw.cache()
      val inputRows = raw.count()
      ctx.setInputRows(inputRows)
      log.info(s"[$runId] 抽取完成，输入行数: $inputRows")

      // ---------- TRANSFORM ----------
      ctx.enterPhase(JobPhase.Transform)
      val transformed = config.transforms.foldLeft(raw) { case (df, tc) =>
        TransformResolver.apply(spark, df, tc.kind, tc.name, tc.params)
      }

      // ---------- QUALITY ----------
      ctx.enterPhase(JobPhase.Quality)
      val outcome: QualityOutcome = config.quality match {
        case Some(qc) if hasRules(qc) =>
          val checker = loadQualityChecker()
          checker.applyQuality(spark, transformed, qc, batchTime)
        case _ =>
          QualityOutcome(cleanDf = transformed, quarantined = 0L,
            total = inputRows, failures = Seq.empty, blocked = false)
      }
      ctx.setQuarantinedRows(outcome.quarantined)
      if (outcome.blocked) {
        val failed = outcome.failures.filterNot(_.passed).map(_.ruleName).mkString(", ")
        throw new QualityBlockException(s"表级质量规则未通过（FAIL 策略）: $failed")
      }

      // ---------- LOAD ----------
      ctx.enterPhase(JobPhase.Load)
      val finalDf = outcome.cleanDf
      config.outputs.foreach { output =>
        val writer = Registry.provider(output.dsType).createWriter(output.mode, output.options)
        writer.write(spark, finalDf)
      }
      finalDf.cache()
      val outputRows = finalDf.count()
      ctx.setOutputRows(outputRows)

      // ---------- FINALIZE ----------
      ctx.enterPhase(JobPhase.Finalize)
      metrics.incCounter("etl.jobs.completed")
      metrics.timer("etl.job.duration.millis",
        java.time.Duration.between(ctx.getStartTime, LocalDateTime.now()).toMillis)
      runStore.foreach(_.success(ctx.record(JobStatus.Succeeded)))
      log.info(s"[$runId] 作业成功: 输入 ${inputRows} 行, 输出 ${outputRows} 行, 隔离 ${outcome.quarantined} 行")
    } catch {
      case NonFatal(e) =>
        metrics.incCounter("etl.jobs.failed")
        log.error("[{}] 作业在 {} 阶段失败: {}", runId, ctx.phase, e.getMessage, e)
        try runStore.foreach(_.failure(ctx.record(JobStatus.Failed, Some(e.getMessage))))
        catch { case NonFatal(storeError) =>
          log.error(s"[$runId] 运行记录写入状态库失败", storeError)
        }
        e match {
          case ex: EtlException => throw ex
          case ex => throw new EtlException(ctx.phase, ex.getMessage, ex)
        }
    } finally {
      // ---------- CLEANUP ----------
      try {
        metrics.close()
      } catch { case NonFatal(e) => log.warn("指标资源释放失败", e) }
      if (spark != null) {
        try spark.stop()
        catch { case NonFatal(e) => log.warn("SparkSession 关闭失败", e) }
      }
      log.info("[{}] 资源清理完成", runId)
    }
  }

  // ------------------------------------------------------------------
  //  私有辅助
  // ------------------------------------------------------------------

  /** 质量配置是否包含任何规则。 */
  private def hasRules(qc: QualityConfig): Boolean =
    qc.rowRules.nonEmpty || qc.tableRules.nonEmpty

  /** 经 ServiceLoader 加载默认 QualityChecker。 */
  private def loadQualityChecker(): QualityChecker = {
    val loaded = ServiceLoader.load(classOf[QualityChecker]).asScala.toSeq
    loaded.headOption.getOrElse(throw new EtlException(JobPhase.Quality,
      "未找到 QualityChecker 实现，请确认 sparketl-quality 模块在 classpath 中"))
  }

  /** 解析运行状态库（monitor 模块经 SPI 提供，未配置时为 None）。 */
  private def resolveRunStore(): Option[com.sparketl.core.spi.JobRunStore] =
    for {
      monitorCfg <- config.monitor
      runStoreCfg <- monitorCfg.runStore
      provider <- ServiceLoader.load(classOf[JobRunStoreProvider]).asScala.toSeq.headOption
    } yield provider.create(RunStoreConfigHolder(
      url = runStoreCfg.url, user = runStoreCfg.user,
      password = runStoreCfg.password, table = runStoreCfg.table))

  /** 解析指标实现（monitor 模块经 SPI 提供，缺省 Noop）。 */
  private def resolveMetrics(): EtlMetrics = {
    val metricsConfig: Option[MetricsConfig] =
      config.monitor.flatMap(_.metrics)
    (metricsConfig, ServiceLoader.load(classOf[MetricsProvider]).asScala.toSeq.headOption) match {
      case (Some(cfg), Some(provider)) =>
        try provider.create(cfg, config.name)
        catch { case NonFatal(e) =>
          log.warn("指标实现初始化失败，回退 NoopMetrics: {}", e.getMessage)
          NoopMetrics
        }
      case _ => NoopMetrics
    }
  }

  private def enterPhase(phase: String): Unit =
    log.info(s"作业 ${config.name} 进入 $phase 阶段")

  private def uuidSuffix(): String =
    java.util.UUID.randomUUID().toString.substring(0, 8)
}
