package com.sparketl.connectors.s3

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import com.sparketl.connectors.hdfs.HdfsProvider
import com.sparketl.core.util.SensitiveDataMasker

/**
 * S3 连接器（providerType = "s3"）。
 *
 * 与 [[HdfsProvider]] 共用 [[com.sparketl.connectors.util.FileStoreLogic]] 的
 * staging + 原子提交逻辑，path 支持 s3a:// 协议。
 *
 * 语义差异：options 中以 fs.s3a. 开头的键（如 fs.s3a.access.key、fs.s3a.secret.key、
 * fs.s3a.endpoint 等）会在读取与写入前注入 spark.sparkContext.hadoopConfiguration，
 * 供 s3a 文件系统客户端使用；其余选项语义与 hdfs 连接器完全一致。
 */
class S3Provider extends HdfsProvider {

  private val log = LoggerFactory.getLogger(classOf[S3Provider])

  override def providerType: String = "s3"

  /** 将 fs.s3a.* 连接选项注入 hadoopConfiguration（读取与写入前均会调用）。 */
  override protected def configureFileSystem(spark: SparkSession, options: Map[String, String]): Unit = {
    val s3Conf = options.collect { case (k, v) if k.startsWith("fs.s3a.") => k -> v }
    if (s3Conf.nonEmpty) {
      val conf = spark.sparkContext.hadoopConfiguration
      s3Conf.foreach { case (k, v) => conf.set(k, v) }
      log.info(s"[s3] 已注入 ${s3Conf.size} 个 fs.s3a.* 配置: " +
        s"${SensitiveDataMasker.maskAll(s3Conf).keys.mkString(", ")}")
    }
  }
}
