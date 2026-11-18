# Checklist

## 工程与构建（R10）
- [x] `mvn clean package` 一键构建全模块通过，sparketl-dist 产出 assembly jar（7/7 模块 BUILD SUCCESS，45 测试全绿）
- [x] 六模块结构清晰（core/connectors/quality/monitor/examples/dist），代码风格统一（阿里巴巴 Scala 规范风格，spotless/scalafmt 可选执行）
- [x] 代码无裸 `println`、无 `System.exit`（仅 CLI 入口收尾退出码）、无明文密码/密钥，注释完整（ScalaDoc）

## Spark 3.5.x 基座（R1）
- [x] Spark 3.5.3 依赖，AQE（含 coalescePartitions、skewJoin）、动态分区裁剪、Parquet/ORC 向量化读取、KryoSerializer 默认开启
- [x] 作业配置可覆盖默认 SparkConf，优先级：作业 YAML > 命令行 > 默认（SparkSessionFactorySpec 断言）
- [x] local 与 yarn 模式均可正常运行，Hive 支持可开关（sparketl.hiveSupport: true|false|auto）

## 作业生命周期（R2）
- [x] 完整生命周期封装：VALIDATE → INIT → EXTRACT → TRANSFORM → QUALITY → LOAD → FINALIZE → CLEANUP
- [x] 非法配置在提交集群前快速失败，错误信息明确（ConfigValidator 聚合全部错误）
- [x] 失败时 staging 清理、运行记录标记 FAILED、CLI 非零退出码（0/1/2）

## 连接器（R3）
- [x] JDBC/Hive/HDFS/S3/Kafka/Iceberg/Delta Lake 均实现统一 Reader/Writer SPI 并经 type 路由（ServiceLoader 注册）
- [x] 敏感项支持 `${env:VAR}` 注入（含默认值语法），日志与运行记录无明文敏感值（SensitiveDataMasker）
- [x] JDBC 支持分区/predicates 并行读取；未配置分片时输出 WARN 性能提示

## 声明式配置（R4）
- [x] 单份 YAML（input/output/transform/quality/retry/monitor）即可运行完整作业，零 Spark 代码
- [x] 内置转换库（SQL/字段操作/过滤/去重/清洗）覆盖常规需求（8 种 kind）
- [x] 多目标输出复用同一转换结果

## 数据质量（R5）
- [x] 行级规则违规数据自动分流至隔离区并附规则名与批次时间（etl_quarantine_rule / etl_quarantine_batch）
- [x] 表级规则（行数/唯一性/notNull/SQL/自定义）支持 FAIL 阻断与 WARN 继续两种策略
- [x] 违规触发日志/Webhook 告警

## 可观测（R6）
- [x] SLF4J/Logback 标准化日志，敏感字段脱敏
- [x] 作业运行记录（状态/起止时间/行数/错误信息）持久化到状态库，附初始化 SQL（sql/sparketl_run_store.sql）
- [x] Spark Listener 指标经 Dropwizard Metrics 输出，Prometheus Sink 可抓取，可对接 Grafana

## 容错与幂等（R7）
- [x] fixed/exponential-backoff 重试策略可配置（含抖动），仅针对可重试异常白名单
- [x] 写入幂等：staging+原子提交（hdfs/s3）/ 动态分区覆盖（hive）/ upsert（jdbc：postgres/mysql/h2 方言）/ 湖表原生事务与 replaceWhere（iceberg/delta），重跑不产生重复数据
- [x] 重试耗尽后清理 staging、记录完整异常（脱敏截断）、作业 FAILED

## 扩展接口（R8）
- [x] `Transformer`（gps_clean 示例）/ `DataSourceProvider`（range 示例）/ `QualityRule`（columnRange 示例）三类 SPI 经 ServiceLoader 注册并在示例与单测中验证可用

## 文档与示例（R9）
- [x] README、快速开始、YAML 配置参考、部署指南齐备且与实现一致
- [x] sparketl-examples 含典型 YAML 作业（jdbc→hive、hive→jdbc、csv→iceberg、kafka→delta、local_range_demo）与三类自定义扩展示例，local 模式全部可运行
- [x] 按文档从零提交示例作业可成功运行（开箱即用，local_range_demo 零外部依赖验证通过）
