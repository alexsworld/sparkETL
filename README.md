# SparkETL

开源的、**配置驱动**的企业级大数据 ETL 框架，基于 Apache Spark 3.5.x（Scala 2.12）。

将传统硬编码的 Spark ETL 作业升级为"**一份 YAML 声明，零 Spark 代码**"的声明式开发模式，
内置企业级生产要素：作业生命周期管理、七大存储系统统一连接器、数据质量校验与隔离、
可配置重试、写入幂等保障、指标监控与运行状态持久化。

本人博客有详细介绍：https://yychan.blog.csdn.net/article/details/170990066

## 核心特性

| 能力 | 说明 |
|------|------|
| Spark 3.5.x 基座 | 默认开启 AQE（分区合并 + 倾斜 Join 优化）、动态分区裁剪、Parquet/ORC 向量化读取、KryoSerializer |
| 生命周期管理 | VALIDATE → INIT → EXTRACT → TRANSFORM → QUALITY → LOAD → FINALIZE → CLEANUP 全流程封装 |
| 七大连接器 | JDBC / Hive / HDFS / S3 / Kafka / Iceberg / Delta Lake 统一 Reader/Writer SPI |
| 声明式配置 | 单份 YAML 描述 input / outputs / transforms / quality / retry / monitor，非法配置提交前快速失败 |
| 数据质量 | 行级规则自动分流隔离区（附规则名与批次时间），表级规则 FAIL 阻断 / WARN 继续，日志与 Webhook 告警 |
| 可观测 | SLF4J 标准化日志（敏感字段脱敏）、JDBC 运行状态库、Prometheus 指标端点、SparkListener 采集 |
| 容错幂等 | fixed / exponential 重试（含抖动）；staging + 原子提交 / 动态分区覆盖 / upsert / 湖表原生事务，重跑不重复 |
| 开放扩展 | Transformer / DataSourceProvider / QualityRule 三类 SPI，ServiceLoader 无缝接入 |

## 工程结构

```
sparketl-parent
├── sparketl-core         # 配置模型/校验、SPI、生命周期、转换库、重试、Session 工厂
├── sparketl-connectors   # jdbc / hive / hdfs / s3 / kafka / iceberg / delta 七大连接器
├── sparketl-quality      # 质量规则引擎、隔离区、告警
├── sparketl-monitor      # JDBC 运行状态库、Prometheus 指标、SparkListener
├── sparketl-examples     # 示例 YAML 作业与三类自定义扩展示例
└── sparketl-dist         # CLI 入口 + assembly 发布 jar
```

## 快速开始

```bash
# 1. 一键构建（JDK 8/11/17 均可，Maven 3.6+）
mvn clean package

# 2. 提交示例作业（零外部依赖，local 模式直接运行）
spark-submit --class com.sparketl.launch.SparkEtlEntrypoint \
  sparketl-dist/target/sparketl-dist-1.0.0.jar \
  --config sparketl-examples/src/main/resources/jobs/local_range_demo.yaml
```

更多内容见文档：

- [快速开始](docs/quickstart.md)
- [YAML 配置参考](docs/config-reference.md)
- [部署指南](docs/deployment.md)

## 一分钟示例

```yaml
name: my_first_etl_job
input:
  type: hdfs
  options: { path: /data/raw/gps, format: csv, header: "true" }
transforms:
  - kind: filter
    params: { condition: "device_id IS NOT NULL" }
quality:
  rowRules:
    - name: coord_valid
      expression: "lon BETWEEN -180 AND 180"
  tableRules:
    - name: row_guard
      kind: minRows
      params: { minRows: "1" }
  onFailure: FAIL
outputs:
  - type: hdfs
    mode: overwrite
    options: { path: /data/ods/gps_clean, format: parquet, partitionBy: "dt" }
retry:
  maxAttempts: 3
  policy: exponential
monitor:
  runStore:
    url: "jdbc:mysql://meta-db:3306/etl"
    user: "${env:ETL_DB_USER}"
    password: "${env:ETL_DB_PASSWORD}"
```

## 环境要求

- 构建侧：JDK 8/11/17、Maven 3.6+
- 运行侧：Spark 3.5.x 集群（YARN / Standalone / local），Java 8/11/17
- 可选：Iceberg（`iceberg-spark-runtime-3.5_2.12`）、Delta Lake（`delta-spark_2.12`）按需通过
  spark-submit 的 packages 参数提供

## License

Apache License 2.0，详见 [LICENSE](LICENSE)。
