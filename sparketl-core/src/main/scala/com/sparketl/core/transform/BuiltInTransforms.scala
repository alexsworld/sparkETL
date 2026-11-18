package com.sparketl.core.transform

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, expr}

import com.sparketl.core.config.ConfigException
import com.sparketl.core.registry.Registry
import com.sparketl.core.spi.Transformer

/**
 * 内置转换器集合，覆盖常规 ETL 转换需求。
 *
 * 各转换器参数约定（来自 YAML transforms[].params）：
 *  - sql            : `sql`（Spark SQL，当前 DataFrame 以 `this` 引用）
 *  - select         : `columns`（逗号分隔列名）
 *  - rename         : `mappings`（`旧列:新列` 对，逗号分隔多组）
 *  - cast           : `mappings`（`列:类型` 对，逗号分隔多组）
 *  - filter         : `condition`（Spark SQL 布尔表达式，仅保留为真行）
 *  - dropDuplicates : `columns`（可选，缺省按全列去重）
 *  - withColumn     : `name` + `expression`
 */
object BuiltInTransforms {

  /** 全部内置转换器实例。 */
  val all: Seq[Transformer] = Seq(
    new SqlTransformer,
    new SelectTransformer,
    new RenameTransformer,
    new CastTransformer,
    new FilterTransformer,
    new DropDuplicatesTransformer,
    new WithColumnTransformer
  )

  private val byName: Map[String, Transformer] = all.map(t => t.name -> t).toMap

  /** 按 kind 获取内置转换器，不存在时返回 None（kind=custom 走 SPI 注册表）。 */
  def byKind(kind: String): Option[Transformer] = byName.get(kind)

  /** 参数必填校验：缺失时抛出 [[ConfigException]]。 */
  private[transform] def requiredParam(params: Map[String, String], key: String, kind: String): String =
    params.get(key).map(_.trim).filter(_.nonEmpty).getOrElse(
      throw new ConfigException(s"转换 '$kind' 缺少必填参数 '$key'"))

  /**
   * 解析 `a:b,c:d` 形式的映射参数（按首个冒号拆分，值中可含冒号如 decimal(10,2)；
   * 逗号在括号内不作为分隔符）。
   */
  private[transform] def parseMappings(raw: String, kind: String): Seq[(String, String)] =
    splitTopLevel(raw).map { pair =>
      val idx = pair.indexOf(':')
      if (idx <= 0) {
        throw new ConfigException(s"转换 '$kind' 参数格式非法，期望 '旧:新' 形式，实际为 '$pair'")
      }
      pair.substring(0, idx).trim -> pair.substring(idx + 1).trim
    }.toSeq

  /** 按括号外的逗号拆分字符串。 */
  private def splitTopLevel(raw: String): Seq[String] = {
    val result = scala.collection.mutable.ListBuffer[String]()
    val sb = new StringBuilder
    var depth = 0
    raw.foreach { ch =>
      ch match {
        case '(' | '[' => depth += 1; sb.append(ch)
        case ')' | ']' => depth -= 1; sb.append(ch)
        case ',' if depth == 0 =>
          if (sb.nonEmpty) result += sb.toString.trim; sb.clear()
        case _ => sb.append(ch)
      }
    }
    if (sb.nonEmpty) result += sb.toString.trim
    result.filter(_.nonEmpty).toSeq
  }
}

/** SQL 表达式转换：当前 DataFrame 以临时视图 `this` 暴露给 SQL。 */
class SqlTransformer extends Transformer {
  override def name: String = "sql"
  override def transform(spark: SparkSession, df: DataFrame, params: Map[String, String]): DataFrame = {
    val sql = BuiltInTransforms.requiredParam(params, "sql", name)
    df.createOrReplaceTempView("this")
    spark.sql(sql)
  }
}

/** 字段选择转换：按逗号分隔列名投影。 */
class SelectTransformer extends Transformer {
  override def name: String = "select"
  override def transform(spark: SparkSession, df: DataFrame, params: Map[String, String]): DataFrame = {
    val columns = BuiltInTransforms.requiredParam(params, "columns", name)
      .split(",").map(_.trim).filter(_.nonEmpty).toSeq
    val unknown = columns.diff(df.columns)
    if (unknown.nonEmpty) {
      throw new ConfigException(s"select 转换包含不存在的列: ${unknown.mkString(", ")}")
    }
    df.select(columns.head, columns.tail: _*)
  }
}

/** 列重命名转换：`mappings` = `旧列:新列`，逗号分隔多组。 */
class RenameTransformer extends Transformer {
  override def name: String = "rename"
  override def transform(spark: SparkSession, df: DataFrame, params: Map[String, String]): DataFrame = {
    val mappings = BuiltInTransforms.parseMappings(
      BuiltInTransforms.requiredParam(params, "mappings", name), name)
    mappings.foldLeft(df) { case (acc, (from, to)) => acc.withColumnRenamed(from, to) }
  }
}

/** 类型转换转换：`mappings` = `列:目标类型`，逗号分隔多组。 */
class CastTransformer extends Transformer {
  override def name: String = "cast"
  override def transform(spark: SparkSession, df: DataFrame, params: Map[String, String]): DataFrame = {
    val mappings = BuiltInTransforms.parseMappings(
      BuiltInTransforms.requiredParam(params, "mappings", name), name)
    mappings.foldLeft(df) { case (acc, (column, dataType)) =>
      acc.withColumn(column, col(column).cast(dataType))
    }
  }
}

/** 过滤转换：保留 `condition` 为真的行（Spark SQL 表达式）。 */
class FilterTransformer extends Transformer {
  override def name: String = "filter"
  override def transform(spark: SparkSession, df: DataFrame, params: Map[String, String]): DataFrame = {
    val condition = BuiltInTransforms.requiredParam(params, "condition", name)
    df.filter(expr(condition))
  }
}

/** 去重转换：按 `columns`（可选）去重，缺省全列去重。 */
class DropDuplicatesTransformer extends Transformer {
  override def name: String = "dropDuplicates"
  override def transform(spark: SparkSession, df: DataFrame, params: Map[String, String]): DataFrame = {
    val columns = params.get("columns").map(_.split(",").map(_.trim).filter(_.nonEmpty).toSeq)
    columns match {
      case Some(cols) => df.dropDuplicates(cols)
      case None       => df.dropDuplicates()
    }
  }
}

/** 新增列转换：`name` 列名 + `expression` Spark SQL 表达式。 */
class WithColumnTransformer extends Transformer {
  override def name: String = "withColumn"
  override def transform(spark: SparkSession, df: DataFrame, params: Map[String, String]): DataFrame = {
    val columnName = BuiltInTransforms.requiredParam(params, "name", name)
    val expression = BuiltInTransforms.requiredParam(params, "expression", name)
    df.withColumn(columnName, expr(expression))
  }
}

/**
 * 转换器路由入口：优先内置 kind，`custom` 时走 SPI 注册表。
 */
object TransformResolver {

  /**
   * 解析并执行单步转换。
   *
   * @param spark  Spark 会话
   * @param df     输入数据
   * @param kind   转换类型
   * @param name   custom 转换器注册名
   * @param params 转换参数
   * @return 转换后的 DataFrame
   */
  def apply(
      spark: SparkSession,
      df: DataFrame,
      kind: String,
      name: Option[String],
      params: Map[String, String]
  ): DataFrame = {
    val transformer =
      if (kind == "custom") Registry.customTransformer(name.getOrElse(kind))
      else BuiltInTransforms.byKind(kind).getOrElse(
        throw new ConfigException(s"未知的转换类型 '$kind'"))
    transformer.transform(spark, df, params)
  }
}
