package com.sparketl.core.transform

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

import com.sparketl.core.config.ConfigException

/** 内置转换链单元测试（local 会话）。 */
class TransformSpec extends AnyFunSuite {

  private lazy val spark: SparkSession = SparkSession.builder()
    .master("local[1]").appName("transform_spec")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  import spark.implicits._

  test("filter + select + withColumn 转换链") {
    val df = Seq((1, "a"), (-2, "b"), (3, "c")).toDF("id", "name")
    val step1 = TransformResolver.apply(spark, df, "filter", None, Map("condition" -> "id > 0"))
    val step2 = TransformResolver.apply(spark, step1, "withColumn", None,
      Map("name" -> "id2", "expression" -> "id * 2"))
    val step3 = TransformResolver.apply(spark, step2, "select", None, Map("columns" -> "id,id2"))
    assert(step3.count() == 2)
    assert(step3.columns.toSeq == Seq("id", "id2"))
  }

  test("rename 与 cast 转换") {
    val df = Seq(("1", 2.5)).toDF("old_col", "amount")
    val renamed = TransformResolver.apply(spark, df, "rename", None,
      Map("mappings" -> "old_col:new_col"))
    assert(renamed.columns.toSeq == Seq("new_col", "amount"))
    val casted = TransformResolver.apply(spark, renamed, "cast", None,
      Map("mappings" -> "amount:decimal(10,2)"))
    assert(casted.schema("amount").dataType.isInstanceOf[org.apache.spark.sql.types.DecimalType])
  }

  test("sql 转换以 this 引用当前 DataFrame") {
    val df = Seq(1, 2, 3).toDF("v")
    val result = TransformResolver.apply(spark, df, "sql", None,
      Map("sql" -> "SELECT sum(v) AS total FROM this"))
    assert(result.head().getLong(0) == 6L)
  }

  test("dropDuplicates 按指定列去重") {
    val df = Seq((1, "a"), (1, "a"), (2, "b")).toDF("id", "name")
    val deduped = TransformResolver.apply(spark, df, "dropDuplicates", None,
      Map("columns" -> "id"))
    assert(deduped.count() == 2)
  }

  test("select 包含不存在列时报错") {
    val df = Seq((1, "a")).toDF("id", "name")
    assertThrows[ConfigException] {
      TransformResolver.apply(spark, df, "select", None, Map("columns" -> "id,ghost"))
    }
  }

  test("custom 转换引用未注册名称时报错") {
    val df = Seq((1, "a")).toDF("id", "name")
    assertThrows[ConfigException] {
      TransformResolver.apply(spark, df, "custom", Some("not_registered"), Map.empty)
    }
  }
}
