---
title: SparkETL 文档
---

# SparkETL

开源的、**配置驱动**的企业级大数据 ETL 框架，基于 Apache Spark 3.5.x（Scala 2.12）。

将传统硬编码的 Spark ETL 作业升级为"**一份 YAML 声明，零 Spark 代码**"的声明式开发模式，内置企业级生产要素：作业生命周期管理、七大存储系统统一连接器、数据质量校验与隔离、可配置重试、写入幂等保障、指标监控与运行状态持久化。

## 文档导航

| 文档 | 说明 |
|------|------|
| [快速开始](quickstart.html) | 10 分钟内完成从构建到首次作业提交 |
| [YAML 配置参考](config-reference.html) | input / outputs / transforms / quality / retry / monitor 全量配置说明 |
| [部署指南](deployment.html) | 发布包构建、YARN / Standalone 提交、生产实践 |

## 核心特性

| 能力 | 说明 |
|------|------|
| Spark 3.5.x 基座 | 默认开启 AQE、动态分区裁剪、Parquet/ORC 向量化读取、KryoSerializer |
| 生命周期管理 | VALIDATE → INIT → EXTRACT → TRANSFORM → QUALITY → LOAD → FINALIZE → CLEANUP 全流程封装 |
| 七大连接器 | JDBC / Hive / HDFS / S3 / Kafka / Iceberg / Delta Lake 统一 Reader/Writer SPI |
| 声明式配置 | 单份 YAML 描述全流程，非法配置提交前快速失败 |
| 数据质量 | 行级规则自动分流隔离区，表级规则 FAIL 阻断 / WARN 继续 |
| 可观测 | SLF4J 标准化日志、JDBC 运行状态库、Prometheus 指标端点 |
| 容错幂等 | fixed / exponential 重试；staging + 原子提交，重跑不重复 |
| 开放扩展 | Transformer / DataSourceProvider / QualityRule 三类 SPI，ServiceLoader 无缝接入 |

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
```

## 环境要求

- 构建侧：JDK 8/11/17、Maven 3.6+
- 运行侧：Spark 3.5.x 集群（YARN / Standalone / local），Java 8/11/17
- 可选：Iceberg、Delta Lake 运行时包按需通过 spark-submit 的 `--packages` 提供

## License

Apache License 2.0，详见仓库 [LICENSE](https://github.com/alexsworld/sparkETL/blob/main/LICENSE)。
