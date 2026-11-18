package com.sparketl.connectors

import java.nio.file.Files

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import com.sparketl.connectors.hdfs.HdfsProvider

/**
 * HDFS / 本地文件连接器测试：基于本地临时目录验证 parquet 读写、
 * append 追加与 overwrite 幂等。
 *
 * Windows 环境无 winutils.exe 时，本地文件系统写 parquet 可能因
 * Hadoop NativeIO 缺失而失败——此类环境问题通过 cancel 跳过测试，
 * 不判失败；真正的逻辑缺陷仍会正常报错。
 */
class HdfsProviderSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var provider: HdfsProvider = _
  private var tempDir: java.nio.file.Path = _

  override def beforeAll(): Unit = {
    tempDir = Files.createTempDirectory("sparketl-hdfs-spec")
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("hdfs-provider-spec")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    provider = new HdfsProvider
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
      spark = null
    }
  }

  /** Windows 本地 FS（缺 winutils / hadoop.dll 错配）导致的写入失败按环境问题跳过。 */
  private def cancelIfWindowsEnvIssue(e: Throwable): Unit = {
    val signature = s"${e.getClass.getName} ${String.valueOf(e.getMessage)}"
    val isEnvIssue = signature.contains("winutils") || signature.contains("UnsatisfiedLinkError") ||
      signature.contains("NativeIO") || signature.contains("HADOOP_HOME")
    if (isEnvIssue) {
      // 环境限制不判失败（UnsatisfiedLinkError 等非 NonFatal，需按 Throwable 兜底捕获）
      cancel(s"Windows 本地文件系统环境限制，跳过: $signature")
    } else throw e
  }

  private def pathOf(name: String): String =
    tempDir.resolve(name).toFile.getAbsolutePath.replace('\\', '/')

  test("parquet 写入后读回并验证内容") {
    try {
      val s = spark
      import s.implicits._
      val path = pathOf("person_parquet")
      val df = Seq((1, "alice"), (2, "bob")).toDF("id", "name")

      provider.createWriter("append", Map("path" -> path, "format" -> "parquet"))
        .write(spark, df)
      val rows = provider.createReader(Map("path" -> path, "format" -> "parquet"))
        .read(spark).collect()

      val byId = rows.map(r => r.getInt(0) -> r.getString(1)).toMap
      assert(byId == Map(1 -> "alice", 2 -> "bob"))
    } catch {
      case e: Throwable => cancelIfWindowsEnvIssue(e)
    }
  }

  test("append 追加写入不覆盖已有数据") {
    try {
      val s = spark
      import s.implicits._
      val path = pathOf("person_append")
      provider.createWriter("append", Map("path" -> path, "format" -> "parquet"))
        .write(spark, Seq((1, "alice")).toDF("id", "name"))
      provider.createWriter("append", Map("path" -> path, "format" -> "parquet"))
        .write(spark, Seq((2, "bob")).toDF("id", "name"))

      assert(provider.createReader(Map("path" -> path, "format" -> "parquet"))
        .read(spark).count() == 2)
    } catch {
      case e: Throwable => cancelIfWindowsEnvIssue(e)
    }
  }

  test("overwrite 重复执行幂等，行数不变") {
    try {
      val s = spark
      import s.implicits._
      val path = pathOf("person_overwrite")
      val writer = provider.createWriter("overwrite", Map("path" -> path, "format" -> "parquet"))
      writer.write(spark, Seq((1, "alice"), (2, "bob")).toDF("id", "name"))
      writer.write(spark, Seq((1, "alice"), (2, "bob")).toDF("id", "name"))

      assert(provider.createReader(Map("path" -> path, "format" -> "parquet"))
        .read(spark).count() == 2)
    } catch {
      case e: Throwable => cancelIfWindowsEnvIssue(e)
    }
  }
}
