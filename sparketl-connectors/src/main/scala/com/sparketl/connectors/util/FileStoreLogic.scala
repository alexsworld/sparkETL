package com.sparketl.connectors.util

import java.util.UUID

import scala.collection.mutable.ListBuffer

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import com.sparketl.core.config.ConfigException
import com.sparketl.core.exception.{EtlException, JobPhase}

/**
 * 文件型存储（HDFS / S3 / 本地）共用的 staging 写入与原子提交逻辑。
 *
 * 提交流程（两阶段）：
 * {{{
 * 1. 将 DataFrame 以 overwrite 语义写入 staging 目录（finalPath + "_staging_" + 随机后缀）；
 * 2. 提交阶段按 mode 处理：
 *    - overwrite：删除已存在的 finalPath 后将 staging 整体重命名过去（幂等，重跑覆盖）；
 *    - append   ：finalPath 不存在时直接整体重命名；已存在时将 staging 下的数据文件
 *                 逐个重命名合并到 finalPath（保留相对路径以维持分区布局，文件名加
 *                 runId 前缀防止与历史文件冲突），完成后 staging 必然为空。
 * }}}
 *
 * 无论成功与否，finally 中都会清理 staging 目录，失败不会污染目标目录。
 */
object FileStoreLogic {

  private val log = LoggerFactory.getLogger(getClass)

  /**
   * staging 写入 + 原子提交。
   *
   * @param spark        Spark 会话（取其 hadoopConfiguration 操作目标文件系统）
   * @param df           待写入数据
   * @param finalPath    最终数据目录（HDFS / S3A / 本地路径均可）
   * @param format       Spark 数据源格式：parquet | orc | csv | json | text
   * @param fmtOptions   透传给 spark.write 的格式选项（如 csv 的 header / delimiter）
   * @param partitionCols 分区列（可为空）
   * @param mode         写入模式：append | overwrite
   * @param extension    该格式对应的数据文件扩展名（用于日志与语义标注）
   */
  def stagingCommit(
      spark: SparkSession,
      df: DataFrame,
      finalPath: String,
      format: String,
      fmtOptions: Map[String, String],
      partitionCols: Seq[String],
      mode: String,
      extension: String): Unit = {

    val hadoopConf = spark.sparkContext.hadoopConfiguration
    // 规范化路径：去除尾部斜杠，保证 staging 目录是 finalPath 的"兄弟"目录
    // （否则 staging 会嵌套在目标目录内部，overwrite 删除目标时会误删 staging）
    val cleanFinal = finalPath.replaceAll("/+$", "")
    val fs = new Path(cleanFinal).getFileSystem(hadoopConf)
    val staging = cleanFinal + "_staging_" + randomSuffix()
    val stagingPath = new Path(staging)
    val finalP = new Path(cleanFinal)

    try {
      // ---------- 1. 先写 staging 目录（overwrite 语义，重跑安全） ----------
      val base = df.write.format(format).mode("overwrite").options(fmtOptions)
      val writer = if (partitionCols.nonEmpty) base.partitionBy(partitionCols: _*) else base
      writer.save(staging)
      log.info(s"staging 写入完成: path=$staging, format=$format$extension, " +
        s"partitionBy=${if (partitionCols.isEmpty) "-" else partitionCols.mkString(",")}")

      // ---------- 2. 原子提交 ----------
      mode match {
        case "overwrite" =>
          if (fs.exists(finalP)) {
            fs.delete(finalP, true)
            log.info(s"覆盖模式：已删除旧数据目录 $finalPath")
          }
          ensureParentDir(fs, finalP)
          renameOrFail(fs, stagingPath, finalP)

        case "append" =>
          if (!fs.exists(finalP)) {
            ensureParentDir(fs, finalP)
            renameOrFail(fs, stagingPath, finalP)
          } else {
            val runId = UUID.randomUUID().toString.substring(0, 8)
            val files = listDataFiles(fs, stagingPath)
            files.foreach { case (src, relParent) =>
              val fileName = runId + "_" + src.getName
              val target = Option(relParent).map(p => new Path(finalP, new Path(p, fileName)))
                .getOrElse(new Path(finalP, fileName))
              Option(relParent).foreach(p => mkdirsIfAbsent(fs, new Path(finalP, p)))
              renameOrFail(fs, src, target)
            }
            log.info(s"追加模式：已合并 ${files.size} 个数据文件到 $finalPath (runId=$runId)")
          }

        case other =>
          throw new ConfigException(
            s"文件型存储不支持的写入模式 '$other'（仅支持 append | overwrite）")
      }
      log.info(s"数据提交成功: path=$finalPath, mode=$mode, format=$format")
    } finally {
      // ---------- 3. 清理 staging（成功后已整体 rename 走，失败时兜底删除） ----------
      try {
        if (fs.exists(stagingPath)) fs.delete(stagingPath, true)
      } catch {
        case e: Exception => log.warn(s"staging 目录清理失败: $staging", e)
      }
    }
  }

  // ------------------------------------------------------------------
  //  私有辅助
  // ------------------------------------------------------------------

  /** 时间戳 + UUID 前 8 位的随机后缀，保证 staging 目录名不冲突。 */
  private def randomSuffix(): String =
    System.currentTimeMillis().toString + "_" + UUID.randomUUID().toString.substring(0, 8)

  /**
   * 递归收集目录下所有数据文件，返回 (文件绝对路径, 相对目录)。
   * 跳过 _SUCCESS / _temporary / .crc 等以 `_` 或 `.` 开头的元数据条目；
   * 相对目录为 null 表示文件位于根（用于 append 合并时保留分区子目录布局）。
   *
   * 注意：刻意使用 listStatus（批量返回 FileStatus）而非 listFiles（LocatedFileStatus
   * 迭代器会在构造时惰性加载权限信息，在 Windows 缺失匹配版 Hadoop native 库时
   * 触发 UnsatisfiedLinkError）；此处仅依赖 isDirectory / getPath，均为普通字段。
   */
  private def listDataFiles(fs: FileSystem, dir: Path): Seq[(Path, Path)] = {
    val buf = ListBuffer.empty[(Path, Path)]
    collectDataFiles(fs, dir, null, buf)
    buf.toSeq
  }

  private def collectDataFiles(
      fs: FileSystem, dir: Path, relParent: Path, buf: ListBuffer[(Path, Path)]): Unit =
    fs.listStatus(dir).foreach { status =>
      val path = status.getPath
      val name = path.getName
      if (name.startsWith("_") || name.startsWith(".")) {
        // 跳过 _SUCCESS / _temporary / .crc 等元数据条目
      } else if (status.isDirectory) {
        val sub = if (relParent == null) new Path(name) else new Path(relParent, name)
        collectDataFiles(fs, path, sub, buf)
      } else {
        buf += ((path, relParent))
      }
    }

  /** rename 前确保目标父目录存在。 */
  private def ensureParentDir(fs: FileSystem, p: Path): Unit =
    mkdirsIfAbsent(fs, p.getParent)

  private def mkdirsIfAbsent(fs: FileSystem, dir: Path): Unit =
    if (dir != null && !fs.exists(dir)) fs.mkdirs(dir)

  /** rename 失败时抛出带阶段信息的异常（Hadoop rename 静默返回 false，必须显式检查）。 */
  private def renameOrFail(fs: FileSystem, src: Path, dst: Path): Unit =
    if (!fs.rename(src, dst)) {
      throw new EtlException(JobPhase.Load, s"文件系统 rename 失败: $src -> $dst")
    }
}
