package com.sparketl.core.session

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

import com.sparketl.core.config.{InputConfig, JobConfig, OutputConfig}

/** SparkSessionFactory 默认优化配置与 Hive 支持开关测试。 */
class SparkSessionFactorySpec extends AnyFunSuite {

  private def sampleJob(sparkConf: Map[String, String] = Map.empty): JobConfig =
    JobConfig(
      name = "factory_spec",
      batchTime = None,
      spark = if (sparkConf.isEmpty) None else Some(sparkConf),
      input = InputConfig("hdfs", Map("path" -> "/in")),
      outputs = Seq(OutputConfig("hdfs", "overwrite", Map("path" -> "/out"))),
      transforms = Seq.empty,
      quality = None,
      retry = None,
      monitor = None
    )

  test("默认启用 AQE / Kryo / 向量化 / 动态分区覆盖") {
    val session = SparkSessionFactory.create(sampleJob(), Map("spark.master" -> "local[1]"))
    try {
      assert(session.conf.get("spark.sql.adaptive.enabled") == "true")
      assert(session.conf.get("spark.sql.adaptive.coalescePartitions.enabled") == "true")
      assert(session.conf.get("spark.sql.adaptive.skewJoin.enabled") == "true")
      assert(session.conf.get("spark.serializer").contains("KryoSerializer"))
      assert(session.conf.get("spark.sql.parquet.enableVectorizedReader") == "true")
      assert(session.conf.get("spark.sql.orc.enableVectorizedReader") == "true")
      assert(session.conf.get("spark.sql.sources.partitionOverwriteMode") == "dynamic")
      assert(session.conf.get("spark.master") == "local[1]")
    } finally session.stop()
  }

  test("作业 YAML 配置优先级高于默认值") {
    val job = sampleJob(sparkConf = Map(
      "spark.master" -> "local[2]",
      "spark.sql.adaptive.enabled" -> "false"))
    val session = SparkSessionFactory.create(job)
    try {
      assert(session.conf.get("spark.master") == "local[2]")
      assert(session.conf.get("spark.sql.adaptive.enabled") == "false")
    } finally session.stop()
  }

  test("auto 模式下 hive 连接器自动启用 Hive 支持") {
    val hiveJob = sampleJob().copy(
      input = InputConfig("hive", Map("table" -> "default.dual")))
    val session = SparkSessionFactory.create(hiveJob)
    try {
      assert(session.conf.get("spark.sql.catalogImplementation") == "hive")
    } finally session.stop()
  }

  test("非 hive 连接器默认不启用 Hive 支持") {
    val session = SparkSessionFactory.create(sampleJob())
    try {
      assert(session.conf.get("spark.sql.catalogImplementation") == "in-memory")
    } finally session.stop()
  }
}
