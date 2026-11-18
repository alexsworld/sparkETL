# YAML 配置参考

作业配置统一使用 YAML，键名 snake_case；敏感值支持 `${env:VAR}` /
`${env:VAR:默认值}` 环境变量占位符（递归解析所有字符串节点）。

## 顶层结构

```yaml
name: <必填，作业名称，全局唯一>
batchTime: <可选，批次时间字符串；缺省为运行时当前时间>
spark: <可选，Map<String,String>，Spark 参数覆盖>
input: <必填，输入数据源>
outputs: <必填，输出目标列表（≥1），复用同一份转换结果>
transforms: <可选，转换链，按顺序执行>
quality: <可选，质量校验>
retry: <可选，重试策略>
monitor: <可选，监控>
```

## spark 段（Spark 参数覆盖）

优先级：**作业 YAML > spark-submit --conf（命令行）> 框架默认**。

```yaml
spark:
  spark.master: yarn                                   # 缺省 local[*]
  spark.sql.shuffle.partitions: "400"
  sparketl.hiveSupport: "true"                         # true | false | auto(缺省)
```

框架默认开启（均可被覆盖）：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| spark.sql.adaptive.enabled | true | AQE 自适应执行 |
| spark.sql.adaptive.coalescePartitions.enabled | true | AQE 自动分区合并 |
| spark.sql.adaptive.skewJoin.enabled | true | AQE 倾斜 Join 优化 |
| spark.serializer | KryoSerializer | 序列化框架 |
| spark.sql.parquet.enableVectorizedReader | true | Parquet 向量化读取 |
| spark.sql.orc.enableVectorizedReader | true | ORC 向量化读取 |
| spark.sql.sources.partitionOverwriteMode | dynamic | 动态分区覆盖（overwrite 仅覆盖相关分区） |

`sparketl.hiveSupport`：`auto`（缺省）时 input / outputs 含 `hive` 连接器即自动启用。

## input 段

```yaml
input:
  type: <连接器类型>       # jdbc | hive | hdfs | s3 | kafka | iceberg | delta
  options: { ... }         # 连接器选项，见下文"连接器选项"
```

## outputs 段

```yaml
outputs:
  - type: <连接器类型>
    mode: append           # append | overwrite | upsert(仅 jdbc)
    options: { ... }
```

多个输出复用同一份（转换 + 质检后的）数据，只计算一次。

## transforms 段（内置转换库）

| kind | 必填参数 | 说明 | 示例 |
|------|---------|------|------|
| `sql` | `sql` | Spark SQL，当前 DataFrame 以 `this` 引用 | `sql: "SELECT *, lon*lat AS x FROM this"` |
| `select` | `columns` | 列投影，逗号分隔 | `columns: "id,name,amount"` |
| `rename` | `mappings` | 列重命名，`旧:新` 逗号分隔多组 | `mappings: "old_col:new_col"` |
| `cast` | `mappings` | 类型转换，`列:类型` 逗号分隔多组 | `mappings: "amount:decimal(10,2),ts:timestamp"` |
| `filter` | `condition` | 保留表达式为真的行 | `condition: "age >= 0"` |
| `dropDuplicates` | `columns`（可选） | 去重；缺省全列 | `columns: "device_id,gps_time"` |
| `withColumn` | `name` + `expression` | 新增计算列 | `name: dt` / `expression: "to_date(ts)"` |
| `custom` | 顶层 `name` | SPI 自定义转换器 | `kind: custom` / `name: gps_clean` |

## quality 段

```yaml
quality:
  rowRules:                          # 行级规则：违规行自动分流隔离区（不阻断）
    - name: not_null_device
      expression: "device_id IS NOT NULL"   # 布尔表达式，为真=合规
  tableRules:                        # 表级规则：整表聚合校验
    - name: row_guard
      kind: minRows                  # minRows | unique | notNull | sql | custom
      params: { minRows: "1" }
    - name: uniq_device
      kind: unique
      params: { columns: "device_id,gps_time" }
    - name: nn_amount
      kind: notNull
      params: { columns: "amount" }
    - name: no_neg_amount
      kind: sql                      # 约定：SQL 查出任何行即违规
      params: { sql: "SELECT * FROM this WHERE amount < 0" }
    - name: age_range
      kind: custom
      params: { rule: "columnRange", column: "age", min: "0", max: "150" }
  onFailure: WARN                    # FAIL=表级规则失败阻断作业 | WARN=记录后继续
  quarantine:                        # 隔离区（可选；行级违规数据写入，附溯源列）
    type: parquet                    # parquet | jdbc
    path: /data/quarantine/gps       # parquet 必填
    options: { url: ..., table: ... }  # jdbc 必填 url；dbtable 取 options.table
  alerts:                            # 告警通道（表级失败时触发）
    - type: log
    - type: webhook
      url: "https://alert.example.com/hook"
```

说明：

- 行级分流会为隔离数据附加 `etl_quarantine_rule`（首个违规规则名）与
  `etl_quarantine_batch`（批次时间）两列；
- 行级规则永不阻断作业；表级规则失败时按 `onFailure` 决定阻断（FAIL）或继续（WARN）；
- 告警仅在存在表级失败结果时触发。

## retry 段

```yaml
retry:
  maxAttempts: 3            # 最大尝试次数（含首次），默认 3
  policy: exponential       # fixed | exponential，默认 fixed
  initialDelayMs: 2000      # 首次重试延迟（毫秒），默认 1000
  multiplier: 2.0           # exponential 退避倍数，默认 2.0
  jitter: true              # ±20% 随机抖动，默认 true
```

重试作用于 EXTRACT 阶段（含数据源连接）；仅对瞬时故障（SQLException、
ConnectException、SocketTimeoutException、UnknownHostException、IOException 等）重试。

## monitor 段

```yaml
monitor:
  runStore:
    url: "jdbc:mysql://meta-db:3306/etl_meta"
    user: "${env:ETL_DB_USER}"
    password: "${env:ETL_DB_PASSWORD}"     # 建议环境变量注入，日志自动脱敏
    table: sparketl_run_store              # 缺省 sparketl_run_store
  metrics:
    prometheusPort: 9091                   # 缺省不开；端点 /metrics
    slf4j: true                            # 每 60s 指标快照日志，默认 false
```

运行记录字段：run_id / job_name / batch_time / status / start_time / end_time /
input_rows / output_rows / quarantined_rows / error_message（已脱敏）。
建表脚本见 `sql/sparketl_run_store.sql`。

## 连接器选项

### jdbc（type: jdbc）

读：`url`、`user`、`password`、`driver`（可选）、`table` 或 `query`（query 优先）、
`fetchsize`；并行分片二选一：`partitionColumn` + `lowerBound` + `upperBound` + `numPartitions`，
或 `predicates`（分号分隔）。未配置分片时输出 WARN 性能提示。

写：`mode: append | overwrite | upsert`；`upsert`（或 append 模式配 `upsertKeys`）
按 `upsertKeys`（逗号分隔）执行 staging 表 + MERGE 语义，支持
postgres（ON CONFLICT）/ mysql（ON DUPLICATE KEY）/ h2（MERGE INTO）方言，重跑幂等。

### hive（type: hive）

读：`table` 或 `query`。写：`table`、`partitionBy`（逗号分隔，动态分区）；
`overwrite` 配合动态分区覆盖仅刷新相关分区，重跑幂等。

### hdfs / s3（type: hdfs / s3）

读：`path`、`format`（parquet 缺省，支持 orc / csv / json / text）、格式选项透传
（csv 的 `header` / `delimiter` 等）。写：`path`、`format`、`partitionBy`（逗号分隔）。

写入统一走 **staging 目录 + 原子提交**：overwrite 删旧再 rename；append 逐文件合并
（runId 前缀防冲突），失败自动清理 staging。`s3` 连接器 `fs.s3a.*` 选项自动注入
Hadoop 配置（endpoint / access key 等建议用 `${env:}` 注入）。

### kafka（type: kafka）

读：`bootstrapServers`（自动映射 `kafka.bootstrap.servers`）、`subscribe` 或 `assign`、
`startingOffsets` / `endingOffsets`（batch 读取）。
写：`bootstrapServers`、`topic`；DataFrame 需含 `key` / `value` 列。

### iceberg（type: iceberg）

读：`path` 或 catalog 标识透传。写：`mode: append | overwrite`；`overwrite` 支持
`replaceWhere` 谓词覆盖；依赖 `spark.sql.catalog.*` 配置（见示例 csv_to_iceberg.yaml）。

### delta（type: delta）

读：`path`。写：`append`；`overwrite` 支持 `replaceWhere`；依赖 Delta 会话扩展配置
（见示例 kafka_to_delta.yaml）。

## 命令行

```
spark-submit --class com.sparketl.launch.SparkEtlEntrypoint <fat.jar> --config <job.yaml>
```

仅 `--config` 一个参数；Spark 参数一律通过 `spark-submit --conf` 或作业 YAML `spark` 段下发。
