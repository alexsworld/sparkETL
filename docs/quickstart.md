# 快速开始

本指南带你在 10 分钟内完成从构建到首次作业提交。

## 1. 环境准备

| 组件 | 版本要求 | 说明 |
|------|---------|------|
| JDK | 8 / 11 / 17 | 构建 JDK 17 时测试 JVM 参数已内置 `--add-opens` |
| Maven | 3.6+ | 用于一键构建 |
| Spark | 3.5.x | local / YARN / Standalone 均可 |

## 2. 构建

```bash
git clone <repo-url> sparketl && cd sparketl
mvn clean package
```

构建产物：

- 各模块 jar：`sparketl-<module>/target/sparketl-<module>-1.0.0.jar`
- 发布 fat jar（推荐提交用）：`sparketl-dist/target/sparketl-dist-1.0.0.jar`
  （已排除 Spark / Scala / Hadoop，避免与集群 classpath 冲突）

运行全部单元测试：`mvn test`（含配置解析、重试、连接器 H2 集成、质量引擎等 45+ 用例）。

## 3. 提交第一个作业（零依赖，local 模式）

框架自带一个不依赖任何外部系统的示例作业（内置 `range` 生成器数据源 → 转换 →
质量校验 → parquet 输出）：

```bash
spark-submit --class com.sparketl.launch.SparkEtlEntrypoint \
  sparketl-dist/target/sparketl-dist-1.0.0.jar \
  --config sparketl-examples/src/main/resources/jobs/local_range_demo.yaml
```

预期输出末尾：

```
... 作业 example_local_range_demo 进入 FINALIZE 阶段
... 作业成功: 输入 100 行, 输出 100 行, 隔离 0 行
```

退出码：0 成功 / 1 作业失败 / 2 参数错误，可直接对接调度系统。

## 4. 编写你自己的作业

新建 `jobs/my_job.yaml`（完整字段说明见 [配置参考](config-reference.md)）：

```yaml
name: my_first_etl_job
spark:
  spark.master: "local[2]"        # 生产提交时删除，由集群指定
input:
  type: hdfs
  options: { path: /data/raw/gps, format: csv, header: "true" }
transforms:
  - kind: filter
    params: { condition: "device_id IS NOT NULL AND lon BETWEEN -180 AND 180" }
  - kind: dropDuplicates
    params: { columns: "device_id,gps_time" }
quality:
  rowRules:
    - name: lat_valid
      expression: "lat BETWEEN -90 AND 90"
  tableRules:
    - name: row_guard
      kind: minRows
      params: { minRows: "1" }
  onFailure: WARN
  quarantine:
    type: parquet
    path: /data/quarantine/gps
outputs:
  - type: hdfs
    mode: overwrite                # staging + 原子提交，重跑幂等
    options:
      path: /data/ods/gps_clean
      format: parquet
      partitionBy: "dt"
retry:
  maxAttempts: 3
  policy: exponential
```

提交：

```bash
spark-submit --class com.sparketl.launch.SparkEtlEntrypoint \
  sparketl-dist/target/sparketl-dist-1.0.0.jar \
  --config jobs/my_job.yaml
```

## 5. 可选：启用监控

运行状态库（一次初始化即可，见 [部署指南](deployment.md#状态库初始化)）：

```bash
mysql -u etl -p etl_meta < sql/sparketl_run_store.sql
```

YAML 中追加：

```yaml
monitor:
  runStore:
    url: "jdbc:mysql://meta-db:3306/etl_meta"
    user: "${env:ETL_DB_USER}"
    password: "${env:ETL_DB_PASSWORD}"
  metrics:
    prometheusPort: 9091           # Prometheus 抓取 http://<driver>:9091/metrics
    slf4j: true                    # 每 60s 输出指标快照日志
```

## 6. 可选：Iceberg / Delta 作业

依赖通过 spark-submit packages 提供：

```bash
# Iceberg
spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2 \
  --class com.sparketl.launch.SparkEtlEntrypoint sparketl-dist/target/sparketl-dist-1.0.0.jar \
  --config sparketl-examples/src/main/resources/jobs/csv_to_iceberg.yaml

# Delta Lake
spark-submit --packages io.delta:delta-spark_2.12:3.1.0 \
  --class com.sparketl.launch.SparkEtlEntrypoint sparketl-dist/target/sparketl-dist-1.0.0.jar \
  --config sparketl-examples/src/main/resources/jobs/kafka_to_delta.yaml
```

## 7. 扩展框架（无需改框架代码）

三类 SPI 均通过 ServiceLoader 注册（在工程的
`src/main/resources/META-INF/services/` 下声明实现类全限定名）：

| SPI 接口 | YAML 引用方式 | 示例 |
|----------|---------------|------|
| `com.sparketl.core.spi.Transformer` | `kind: custom` + `name: 注册名` | `sparketl-examples` 的 `gps_clean`（旧 GPS 清洗逻辑迁移） |
| `com.sparketl.core.spi.DataSourceProvider` | `input.type` / `outputs[].type` | `sparketl-examples` 的 `range` 生成器 |
| `com.sparketl.core.spi.QualityRule` | `kind: custom` + `params.rule: 注册名` | `sparketl-examples` 的 `columnRange` |

将自定义扩展与作业一起打进 fat jar 即可生效，参见 `sparketl-examples` 模块源码。
