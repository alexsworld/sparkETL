package com.sparketl.connectors

import java.sql.DriverManager

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import com.sparketl.connectors.jdbc.JdbcProvider
import com.sparketl.core.registry.Registry

/**
 * JDBC 连接器集成测试：基于 H2 内存库验证 append / upsert / 幂等语义。
 *
 * URL 附带 DATABASE_TO_UPPER=FALSE，使 H2 保持小写标识符，
 * 与连接器合并 SQL 中的双引号引用保持一致。
 */
class JdbcProviderSpec extends AnyFunSuite with BeforeAndAfterAll {

  private val Url = "jdbc:h2:mem:jdbctest;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=FALSE"
  private val Driver = "org.h2.Driver"

  private var spark: SparkSession = _
  private var provider: JdbcProvider = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("jdbc-provider-spec")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    provider = new JdbcProvider

    // 初始化 t_test 表结构
    val conn = DriverManager.getConnection(Url, "sa", "")
    try {
      val drop = conn.createStatement()
      try drop.execute("DROP TABLE IF EXISTS t_test")
      finally drop.close()
      val create = conn.createStatement()
      try create.execute("CREATE TABLE t_test (id INT, name VARCHAR(32))")
      finally create.close()
    } finally conn.close()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
      spark = null
    }
  }

  private def writeOptions(mode: String, extra: Map[String, String] = Map.empty): Map[String, String] =
    Map("url" -> Url, "user" -> "sa", "password" -> "", "driver" -> Driver, "table" -> "t_test") ++ extra

  private def readOptions: Map[String, String] =
    Map("url" -> Url, "user" -> "sa", "password" -> "", "driver" -> Driver, "table" -> "t_test")

  private def rowsById: Map[Int, String] = {
    val rows = provider.createReader(readOptions).read(spark).collect()
    rows.map(r => r.getInt(0) -> r.getString(1)).toMap
  }

  test("SPI 注册可解析 jdbc 连接器") {
    assert(Registry.provider("jdbc").providerType == "jdbc")
  }

  test("append 写入 2 行并可读回验证内容") {
    val s = spark
    import s.implicits._
    val df = Seq((1, "alice"), (2, "bob")).toDF("id", "name")
    provider.createWriter("append", writeOptions("append")).write(spark, df)

    val byId = rowsById
    assert(byId.size == 2)
    assert(byId(1) == "alice")
    assert(byId(2) == "bob")
  }

  test("upsert 按 id 合并：1 行更新 + 1 行新增，共 3 行") {
    val s = spark
    import s.implicits._
    val df = Seq((1, "alice2"), (3, "carol")).toDF("id", "name")
    provider.createWriter("upsert", writeOptions("upsert", Map("upsertKeys" -> "id")))
      .write(spark, df)

    val byId = rowsById
    assert(byId.size == 3)
    assert(byId(1) == "alice2")
    assert(byId(2) == "bob")
    assert(byId(3) == "carol")
  }

  test("upsert 重复执行幂等，仍为 3 行") {
    val s = spark
    import s.implicits._
    val df = Seq((1, "alice2"), (3, "carol")).toDF("id", "name")
    val writer = provider.createWriter("upsert", writeOptions("upsert", Map("upsertKeys" -> "id")))
    writer.write(spark, df)
    writer.write(spark, df)

    assert(rowsById.size == 3)
  }
}
