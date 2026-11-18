package com.sparketl.quality.rules

import java.util.ServiceLoader

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

import com.sparketl.core.config.ConfigException
import com.sparketl.core.spi.{QualityCheckResult, QualityRule}

/**
 * 表级规则 minRows：校验数据行数不低于下限。
 *
 * 参数约定（来自 YAML tableRules[].params）：
 *  - `minRows`：允许的最小行数（正整数，字符串形式）
 *
 * 度量口径：metric 为实际行数；实际行数 >= minRows 视为通过。
 */
class MinRowsRule extends QualityRule {

  override def name: String = "minRows"

  override def evaluate(spark: SparkSession, df: DataFrame, params: Map[String, String]): QualityCheckResult = {
    val minRows = BuiltInTableRules.parseLong(
      BuiltInTableRules.requiredParam(params, "minRows", name), name)
    val count = df.count()
    if (count >= minRows) {
      QualityCheckResult(name, passed = true, count, s"行数校验通过: 实际行数 $count >= 下限 $minRows")
    } else {
      QualityCheckResult(name, passed = false, count, s"行数校验失败: 实际行数 $count 低于下限 $minRows")
    }
  }
}

/**
 * 表级规则 unique：校验指定列（组合）无重复值。
 *
 * 参数约定：
 *  - `columns`：逗号分隔的列名列表
 *
 * 度量口径：metric 为重复行数（总行数 - 去重后行数）；重复数为 0 视为通过。
 */
class UniqueRule extends QualityRule {

  override def name: String = "unique"

  override def evaluate(spark: SparkSession, df: DataFrame, params: Map[String, String]): QualityCheckResult = {
    val columns = BuiltInTableRules.parseColumns(params, name)
    val duplicates = df.count() - df.dropDuplicates(columns).count()
    if (duplicates == 0L) {
      QualityCheckResult(name, passed = true, 0L, s"唯一性校验通过: 列 [${columns.mkString(", ")}] 无重复行")
    } else {
      QualityCheckResult(name, passed = false, duplicates, s"唯一性校验失败: 列 [${columns.mkString(", ")}] 存在 $duplicates 条重复行")
    }
  }
}

/**
 * 表级规则 notNull：校验指定列不含 null 或空字符串。
 *
 * 参数约定：
 *  - `columns`：逗号分隔的列名列表
 *
 * 度量口径：metric 为各列 null/空串行数之和；合计为 0 视为通过。
 */
class NotNullRule extends QualityRule {

  override def name: String = "notNull"

  override def evaluate(spark: SparkSession, df: DataFrame, params: Map[String, String]): QualityCheckResult = {
    val columns = BuiltInTableRules.parseColumns(params, name)
    val violations = columns.map(c => df.filter(col(c).isNull || col(c) === "").count()).sum
    if (violations == 0L) {
      QualityCheckResult(name, passed = true, 0L, s"非空校验通过: 列 [${columns.mkString(", ")}] 无 null/空串")
    } else {
      QualityCheckResult(name, passed = false, violations, s"非空校验失败: 列 [${columns.mkString(", ")}] 共 $violations 行为 null/空串")
    }
  }
}

/**
 * 表级规则 sql：以 SQL 表达复杂校验逻辑，查出任何行即违规。
 *
 * 参数约定：
 *  - `sql`：Spark SQL 查询，当前 DataFrame 以临时视图 `this` 引用，
 *    如 `SELECT * FROM this WHERE age < 0`
 *
 * 度量口径：metric 为查询返回行数；返回 0 行视为通过。
 */
class SqlTableRule extends QualityRule {

  override def name: String = "sql"

  override def evaluate(spark: SparkSession, df: DataFrame, params: Map[String, String]): QualityCheckResult = {
    val sql = BuiltInTableRules.requiredParam(params, "sql", name)
    df.createOrReplaceTempView("this")
    val violations = spark.sql(sql).count()
    if (violations == 0L) {
      QualityCheckResult(name, passed = true, 0L, "SQL 校验通过: 无违规行")
    } else {
      QualityCheckResult(name, passed = false, violations, s"SQL 校验失败: 查出 $violations 行违规数据")
    }
  }
}

/**
 * 内置表级规则注册表，覆盖常规表级校验需求。
 *
 * 规则类型（YAML tableRules[].kind）：
 *  - `minRows` / `unique` / `notNull` / `sql`：内置规则
 *  - `custom`：自定义规则，经 Java ServiceLoader 注册，
 *    以 `params.rule` 指定 SPI 注册名
 */
object BuiltInTableRules {

  /** 全部内置表级规则实例。 */
  val all: Seq[QualityRule] = Seq(new MinRowsRule, new UniqueRule, new NotNullRule, new SqlTableRule)

  private val byName: Map[String, QualityRule] = all.map(r => r.name -> r).toMap

  /** 按 kind 获取内置表级规则，不存在时返回 None（kind=custom 走 SPI 加载）。 */
  def byKind(kind: String): Option[QualityRule] = byName.get(kind)

  /** 参数必填校验：缺失或为空白时抛出 [[ConfigException]]。 */
  private[rules] def requiredParam(params: Map[String, String], key: String, kind: String): String =
    params.get(key).map(_.trim).filter(_.nonEmpty).getOrElse(
      throw new ConfigException(s"表级规则 '$kind' 缺少必填参数 '$key'"))

  /** 解析逗号分隔的列名参数，剔除空白项且不允许为空。 */
  private[rules] def parseColumns(params: Map[String, String], kind: String): Seq[String] = {
    val columns = requiredParam(params, "columns", kind).split(",").map(_.trim).filter(_.nonEmpty).toSeq
    if (columns.isEmpty) throw new ConfigException(s"表级规则 '$kind' 参数 'columns' 不能为空")
    columns
  }

  /** 解析 Long 型参数，格式非法时抛出 [[ConfigException]]。 */
  private[rules] def parseLong(raw: String, kind: String): Long =
    try raw.toLong
    catch {
      case NonFatal(_) => throw new ConfigException(s"表级规则 '$kind' 参数格式非法，期望数字，实际为 '$raw'")
    }
}

/**
 * 自定义表级规则加载器：通过 Java ServiceLoader 发现 SPI 注册的 [[QualityRule]] 实现。
 *
 * 自定义实现需在 jar 包的 `META-INF/services/com.sparketl.core.spi.QualityRule`
 * 文件中登记，YAML 侧以 `kind: custom` + `params.rule: 注册名` 引用。
 */
object CustomRuleLoader {

  /**
   * 按注册名加载自定义规则实现。
   *
   * @param name SPI 注册名（与实现类的 `name` 一致）
   * @return 匹配的规则实例
   * @throws ConfigException 未找到对应注册名时抛出
   */
  def load(name: String): QualityRule =
    ServiceLoader.load(classOf[QualityRule]).asScala.find(_.name == name).getOrElse(
      throw new ConfigException(s"未找到注册名为 '$name' 的自定义质量规则，请确认已在 META-INF/services 中登记"))
}
