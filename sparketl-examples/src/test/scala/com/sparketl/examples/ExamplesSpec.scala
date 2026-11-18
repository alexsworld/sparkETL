package com.sparketl.examples

import java.nio.file.Files

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

import com.sparketl.core.config.{ConfigParser, OutputConfig}
import com.sparketl.core.lifecycle.JobLifecycle
import com.sparketl.examples.provider.RangeProvider
import com.sparketl.examples.rule.ColumnRangeRule
import com.sparketl.examples.transform.GpsCleanTransformer

/** 扩展示例测试：自定义转换器 / 自定义质量规则 / 自定义数据源 + 本地端到端作业。 */
class ExamplesSpec extends AnyFunSuite {

  private lazy val spark: SparkSession = SparkSession.builder()
    .master("local[2]").appName("examples_spec")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  import spark.implicits._

  test("GpsCleanTransformer 清洗非法坐标并去重") {
    val df = Seq(
      ("D001", "2026-11-18 09:00:00", 116.397, 39.908),
      ("D001", "2026-11-18 09:00:00", 116.397, 39.908), // 重复
      ("D003", "2026-11-18 09:02:10", 0.0, 0.0),        // 全零
      ("D004", "2026-11-18 09:03:45", 999.0, 39.9),     // 越界
      ("D005", "2026-11-18 09:04:20", 104.065, 30.659)
    ).toDF("device_id", "gps_time", "lon", "lat")

    val cleaned = new GpsCleanTransformer().transform(spark, df, Map.empty)
    assert(cleaned.count() == 2) // D001 与 D005
  }

  test("ColumnRangeRule 检测越界值") {
    val df = Seq(5, 200, -3).toDF("age")
    val result = new ColumnRangeRule().evaluate(spark, df,
      Map("column" -> "age", "min" -> "0", "max" -> "150"))
    assert(!result.passed)
    assert(result.metric == 2L)
  }

  test("RangeProvider 读写闭环") {
    val provider = new RangeProvider()
    assert(provider.providerType == "range")
    val read = provider.createReader(Map("start" -> "0", "end" -> "10"))
    assert(read.read(spark).count() == 10L)
  }

  test("local_range_demo.yaml 端到端运行（range -> 转换 -> 质量 -> parquet）") {
    val yamlStream = getClass.getResourceAsStream("/jobs/local_range_demo.yaml")
    assert(yamlStream != null, "示例 YAML 应存在于 resources/jobs 下")
    val yaml = scala.io.Source.fromInputStream(yamlStream, "UTF-8").mkString
    val config = ConfigParser.parse(yaml)
      .copy(outputs = Seq(OutputConfig(
        "hdfs", "overwrite",
        Map("path" -> Files.createTempDirectory("etl_demo").toUri.toString, "format" -> "parquet"))))
    // 端到端执行不抛异常即通过
    new JobLifecycle(config).run()
  }
}
