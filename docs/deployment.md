# 部署指南

## 1. 构建发布包

```bash
mvn clean package -DskipTests   # 生产发布建议先完整跑一次 mvn test
```

产物：`sparketl-dist/target/sparketl-dist-1.0.0.jar`（fat jar，已排除 Spark / Scala /
Hadoop 依赖；含全部连接器、质量、监控模块与 SPI 注册文件）。

## 2. YARN 集群提交

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  --class com.sparketl.launch.SparkEtlEntrypoint \
  --num-executors 8 \
  --executor-memory 4g \
  --executor-cores 2 \
  --conf spark.sql.shuffle.partitions=400 \
  sparketl-dist/target/sparketl-dist-1.0.0.jar \
  --config hdfs:///etl/jobs/gps_clean.yaml     # 建议配置文件放 HDFS
```

要点：

- **Spark 参数下发**：`spark-submit --conf` 为全局基线；作业 YAML `spark` 段优先级更高，
  可按作业覆盖（如 shuffle 分区数）；
- **Hive 作业**：确保提交环境有 `hive-site.xml`（SPARK_CONF_DIR 或 --files 下发）；
  YAML 无需额外开关（`sparketl.hiveSupport` 默认 auto 自动启用）；
- **cluster 模式**：`--deploy-mode cluster` 时配置文件必须位于集群存储（HDFS/S3/OSS），
  Prometheus 指标端点随 driver 所在节点漂移，建议配合固定端口策略或改用 JMX/SLF4J 输出。

### 依赖型连接器的 packages 下发

| 连接器 | packages 参数 |
|--------|---------------|
| Iceberg | `org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2` |
| Delta Lake | `io.delta:delta-spark_2.12:3.1.0` |
| MySQL | `mysql:mysql-connector-java:8.0.33` |
| PostgreSQL | `org.postgresql:postgresql:42.7.3` |
| Kafka | `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3` |

示例：

```bash
spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2 \
  --class com.sparketl.launch.SparkEtlEntrypoint \
  sparketl-dist/target/sparketl-dist-1.0.0.jar \
  --config hdfs:///etl/jobs/csv_to_iceberg.yaml
```

## 3. 状态库初始化

选择一个关系库作为元数据/运行状态库（MySQL / PostgreSQL / H2 均可）：

```bash
mysql -u etl -p -h meta-db etl_meta < sql/sparketl_run_store.sql
```

为框架账号授权（最小权限：目标状态库的 SELECT/INSERT/UPDATE/DELETE/CREATE）：

```sql
CREATE USER 'etl'@'%' IDENTIFIED BY '<password>';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE ON etl_meta.* TO 'etl'@'%';
```

作业 YAML 中通过环境变量注入凭据（日志与运行记录自动脱敏）：

```yaml
monitor:
  runStore:
    url: "jdbc:mysql://meta-db:3306/etl_meta"
    user: "${env:ETL_RUNSTORE_USER}"
    password: "${env:ETL_RUNSTORE_PASSWORD}"
```

说明：状态库不可用时作业**不会失败**——运行记录写入失败仅记录 ERROR 日志，
不阻断 ETL 主流程。

## 4. Prometheus / Grafana 对接

### 指标暴露

```yaml
monitor:
  metrics:
    prometheusPort: 9091
```

driver 进程在 9091 端口暴露 `/metrics`，指标包括：

| 指标 | 类型 | 说明 |
|------|------|------|
| etl_jobs_completed_total / etl_jobs_failed_total | counter | 作业成功/失败次数 |
| etl_input_rows / etl_output_rows / etl_quarantined_rows | gauge | 输入/输出/隔离行数 |
| etl_job_duration_total_ms | gauge | 作业累计耗时（毫秒） |
| spark_stages_completed_total / spark_tasks_failed_total | counter | Spark stage / 失败 task 计数 |
| spark_stage_duration_total_ms | gauge | stage 累计耗时 |

### Prometheus 采集配置

```yaml
scrape_configs:
  - job_name: sparketl
    static_configs:
      - targets: ['<driver-host>:9091']
```

（client/固定端口场景适用；cluster 模式建议配合 Spark on K8s 的 pod discovery，
或退化为 `slf4j: true` 的周期快照日志采集。）

### Grafana 常用看板

- 作业成功率：`sum(rate(etl_jobs_completed_total[1h])) / (sum(rate(etl_jobs_completed_total[1h])) + sum(rate(etl_jobs_failed_total[1h])))`
- 数据倾斜排查：`etl_input_rows - etl_output_rows - etl_quarantined_rows`（应接近 0）
- 作业耗时趋势：`etl_job_duration_total_ms`（结合 run store 按作业名分组）

## 5. 调度系统集成

| 调度器 | 集成方式 |
|--------|---------|
| Airflow | `SparkSubmitOperator`，`exit` 0/1/2 即任务成功/失败/参数错误 |
| DolphinScheduler | Shell 任务包装 spark-submit，依赖退出码 |
| Azkaban / Oozie | 原生 spark / shell job 类型 |

作业级 SLA 数据直接查询状态库：

```sql
SELECT job_name, status, start_time, end_time,
       TIMESTAMPDIFF(SECOND, start_time, end_time) AS duration_s,
       input_rows, output_rows, quarantined_rows, error_message
FROM sparketl_run_store
WHERE start_time > NOW() - INTERVAL 24 HOUR
ORDER BY start_time DESC;
```

## 6. 生产检查清单

- [ ] 作业 YAML 中所有凭据改用 `${env:VAR}` 注入，无明文密码
- [ ] 输出作业均选择幂等写入模式（overwrite 动态分区 / jdbc upsert / 湖表 replaceWhere）
- [ ] 配置了 `retry`（建议 exponential + 3 次）以抵御数据源瞬时抖动
- [ ] 关键作业配置 `quality.tableRules` + `onFailure: FAIL` + webhook 告警
- [ ] 隔离区目录设置生命周期清理策略（如 30 天）
- [ ] 状态库 `sparketl_run_store` 建表并纳入备份
- [ ] Prometheus 抓取与 Grafana 告警规则配置完毕
- [ ] 首次上线前在 local / 测试集群完整重跑一次验证幂等（连续提交两次，行数不翻倍）

## 7. 常见问题

**Q：Windows 本地调试 parquet 报 winutils/native 错误？**
A：框架写入逻辑已规避 native 依赖路径（listStatus 而非 listFiles），绝大多数场景无需
winutils；如仍遇到，可安装 winutils 或改在 WSL/Linux 验证。

**Q：JDK 17 提交报 `InaccessibleObjectException`？**
A：追加 `--conf spark.driver.extraJavaOptions="--add-opens=java.base/java.lang=ALL-UNNAMED ..."`
（完整参数列表见根 pom.xml 的 `test.jvm.args` 属性）。

**Q：JDBC upsert 不支持我的数据库？**
A：当前内置 postgres / mysql / h2 三种方言；其他库请改用 `mode: append` +
下游去重，或基于 SPI 自定义连接器。

**Q：如何验证写入幂等？**
A：同一作业连续提交两次：文件型 overwrite / jdbc upsert / 湖表 replaceWhere 场景下
目标行数应保持不变；append 模式为追加语义（跨批次数据按批次隔离，非重复写入）。
