package com.sparketl.core.config

import org.scalatest.funsuite.AnyFunSuite

import com.sparketl.core.util.SensitiveDataMasker

/** 配置解析 / 环境变量占位符 / 校验器单元测试。 */
class ConfigParserSpec extends AnyFunSuite {

  private val ValidYaml =
    """name: test_job
      |spark:
      |  spark.master: local[2]
      |input:
      |  type: hdfs
      |  options:
      |    path: /data/in
      |outputs:
      |  - type: hdfs
      |    mode: overwrite
      |    options:
      |      path: /data/out
      |transforms:
      |  - kind: filter
      |    params:
      |      condition: "id > 0"
      |  - kind: custom
      |    name: my_transform
      |quality:
      |  rowRules:
      |    - name: not_null
      |      expression: "id IS NOT NULL"
      |  tableRules:
      |    - name: row_guard
      |      kind: minRows
      |      params: { minRows: "1" }
      |  onFailure: FAIL
      |  quarantine:
      |    type: parquet
      |    path: /data/quarantine
      |  alerts:
      |    - type: log
      |retry:
      |  maxAttempts: 5
      |  policy: exponential
      |monitor:
      |  runStore:
      |    url: "jdbc:h2:mem:runstore"
      |  metrics:
      |    prometheusPort: 9091
      |""".stripMargin

  test("解析合法完整配置") {
    val cfg = ConfigParser.parse(ValidYaml)
    assert(cfg.name == "test_job")
    assert(cfg.input.dsType == "hdfs")
    assert(cfg.input.options("path") == "/data/in")
    assert(cfg.outputs.size == 1)
    assert(cfg.outputs.head.mode == "overwrite")
    assert(cfg.transforms.size == 2)
    assert(cfg.transforms(1).kind == "custom")
    assert(cfg.transforms(1).name.contains("my_transform"))
    val quality = cfg.quality.get
    assert(quality.onFailure == "FAIL")
    assert(quality.rowRules.head.expression == "id IS NOT NULL")
    assert(quality.quarantine.get.path.contains("/data/quarantine"))
    assert(cfg.retry.get.maxAttempts == 5)
    assert(cfg.retry.get.policy == "exponential")
    assert(cfg.monitor.get.metrics.get.prometheusPort.contains(9091))
    assert(cfg.spark.get("spark.master").contains("local[2]"))
  }

  test("缺失必填项 name 时快速失败") {
    val yaml = ValidYaml.replace("name: test_job\n", "")
    val ex = intercept[ConfigException](ConfigParser.parse(yaml))
    assert(ex.getMessage.contains("name"))
  }

  test("outputs 缺失时快速失败") {
    val yaml = ValidYaml.replaceAll("(?s)outputs:.*?transforms:", "transforms:")
    assertThrows[ConfigException](ConfigParser.parse(yaml))
  }

  test("env 占位符未设置且无默认值时快速失败") {
    val yaml =
      """name: env_job
        |input:
        |  type: jdbc
        |  options:
        |    url: "${env:ETL_DEFINITELY_NOT_SET_VAR}"
        |outputs:
        |  - type: hdfs
        |    options: { path: "/out" }
        |""".stripMargin
    val e = intercept[ConfigException](ConfigParser.parse(yaml))
    assert(e.getMessage.contains("ETL_DEFINITELY_NOT_SET_VAR"))
  }

  test("env 占位符带默认值时使用默认值") {
    val yaml =
      """name: env_default_job
        |input:
        |  type: jdbc
        |  options:
        |    url: "${env:ETL_NO_SUCH_VAR:jdbc:h2:mem:default}"
        |outputs:
        |  - type: hdfs
        |    options: { path: "/out" }
        |""".stripMargin
    val cfg = ConfigParser.parse(yaml)
    assert(cfg.input.options("url") == "jdbc:h2:mem:default")
  }

  test("校验器拒绝非法输出模式与非法转换类型") {
    val yaml = ValidYaml.replace("mode: overwrite", "mode: bad_mode")
      .replace("kind: filter", "kind: no_such_kind")
    val cfg = ConfigParser.parse(yaml)
    val errors = ConfigValidator.validateAll(cfg)
    assert(errors.exists(_.contains("mode")))
    assert(errors.exists(_.contains("kind")))
    assertThrows[ConfigException](ConfigValidator.validate(cfg))
  }

  test("校验器拒绝 custom 转换缺少 name") {
    val yaml = ValidYaml.replace(
      "  - kind: custom\n    name: my_transform\n", "  - kind: custom\n")
    val cfg = ConfigParser.parse(yaml)
    assert(ConfigValidator.validateAll(cfg).nonEmpty)
  }

  test("校验器拒绝 FAIL/WARN 之外的 onFailure 与非法表级规则 kind") {
    val yaml = ValidYaml.replace("onFailure: FAIL", "onFailure: MAYBE")
      .replace("kind: minRows", "kind: whatever")
    val errors = ConfigValidator.validateAll(ConfigParser.parse(yaml))
    assert(errors.exists(_.contains("onFailure")))
    assert(errors.exists(_.contains("tableRules")))
  }

  test("数字标量自动转字符串选项") {
    val yaml =
      """name: num_job
        |input:
        |  type: hdfs
        |  options: { path: /in }
        |outputs:
        |  - type: hdfs
        |    options: { path: /out, parallelism: 8 }
        |""".stripMargin
    val cfg = ConfigParser.parse(yaml)
    assert(cfg.outputs.head.options("parallelism") == "8")
  }
}

/** 敏感信息脱敏测试。 */
class SensitiveDataMaskerSpec extends AnyFunSuite {
  test("password 等敏感键值被脱敏") {
    assert(SensitiveDataMasker.mask("password", "secret123") == "se***")
    assert(SensitiveDataMasker.mask("jdbcUrl", "jdbc:h2:mem") == "jdbc:h2:mem")
    assert(SensitiveDataMasker.mask("accessToken", "tk") == "***")
  }
}
