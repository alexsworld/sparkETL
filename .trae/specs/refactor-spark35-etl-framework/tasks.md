# Tasks

- [x] Task 1: 搭建 Maven 多模块工程骨架与构建基线
  - [x] SubTask 1.1: 创建父 POM（Spark 3.5.x / Scala 2.12 / Java 8+17 兼容、统一依赖与插件管理）及 sparketl-core / sparketl-connectors / sparketl-quality / sparketl-monitor / sparketl-examples / sparketl-dist 六个子模块
  - [x] SubTask 1.2: 配置 scalafmt + scalastyle（阿里巴巴 Scala 规则子集）、spotless、assembly 插件；`.gitignore`、`LICENSE`（Apache 2.0）
  - [x] 验证：`mvn clean package` 全模块构建通过，dist 产出 assembly jar

- [x] Task 2: sparketl-core —— 配置模型与校验（R4 前置）
  - [x] SubTask 2.1: 定义作业配置 case class 模型（JobConfig / InputConfig / TransformConfig / QualityConfig / RetryConfig / MonitorConfig / SparkConfConfig），基于 SnakeYAML 解析，支持 `${env:VAR}` 敏感占位符
  - [x] SubTask 2.2: 实现配置校验器（必填项/类型/枚举/组合合法性），非法配置提交前快速失败，错误信息明确
  - [x] 验证：单元测试覆盖合法/非法配置解析与校验

- [x] Task 3: sparketl-core —— SparkSession 工厂与 3.5.x 默认优化（R1）
  - [x] SubTask 3.1: 实现 SparkSessionFactory：默认启用 AQE（coalesce/skewJoin）、DPF、向量化读取、KryoSerializer；配置覆盖优先级（作业 YAML > 命令行 > 默认）
  - [x] SubTask 3.2: local 与 yarn 模式自适应，enableHiveSupport 可配置开关
  - [x] 验证：单测断言默认 SparkConf；local 模式冒烟运行通过

- [x] Task 4: sparketl-core —— Reader/Writer SPI 与连接器注册表（R3 核心，sparketl-connectors）
  - [x] SubTask 4.1: 定义 `Reader`/`Writer`/`DataSourceProvider` SPI 与 DataSourceRegistry（ServiceLoader 加载 + type 路由）
  - [x] SubTask 4.2: 实现 JDBC 连接器（分区列/predicates 并行读、fetchsize、upsert 幂等写、连接参数模型与敏感脱敏日志）
  - [x] SubTask 4.3: 实现 Hive 与 HDFS 连接器（Parquet/ORC/CSV/JSON/Text 读写，动态分区、分区覆盖幂等，替代旧 TaskOutput.hive/parquet）
  - [x] SubTask 4.4: 实现 S3A、Kafka、Iceberg、Delta Lake 连接器（依赖 provided/optional，统一 options 透传）
  - [x] 验证：local + 内嵌 HSQLDB/临时目录对 JDBC/HDFS/Parquet 读写做集成冒烟

- [x] Task 5: sparketl-core —— 生命周期状态机与通用执行引擎（R2）
  - [x] SubTask 5.1: 实现 JobLifecycle（init → extract → transform → load → finalize）+ 作业级异常体系，禁用 System.exit
  - [x] SubTask 5.2: 实现内置 Transform 库：SQL 表达式、字段选择/重命名/类型转换、filter、dropDuplicates、行级清洗（替代旧 GPS Executor 中的硬编码逻辑）；多目标输出复用同一转换结果
  - [x] SubTask 5.3: 实现 staging 临时目录 + 原子提交的通用幂等 Load 流程，失败自动回滚清理
  - [x] 验证：端到端单测——示例 YAML（range→transform→quality→hdfs）在 local 模式跑通并二次运行无重复

- [x] Task 6: sparketl-core —— 重试策略（R7 前半）
  - [x] SubTask 6.1: 实现 RetryPolicy（fixed / exponential-backoff + 抖动、最大次数、可重试异常白名单），包装资源初始化与 Extract 阶段
  - [x] 验证：单测模拟连接失败 N 次后成功/重试耗尽两条路径

- [x] Task 7: sparketl-quality —— 数据质量模块（R5）
  - [x] SubTask 7.1: 实现 `QualityRule` SPI 与内置规则：行级（null/范围/正则/SQL 表达式）、表级（行数阈值、唯一性、聚合比对）
  - [x] SubTask 7.2: 实现异常数据自动分流（quarantine 隔离区写入，附规则名与批次时间）、策略（FAIL 阻断/WARN 继续）
  - [x] SubTask 7.3: 实现告警通道（日志 + HTTP Webhook）
  - [x] 验证：单测覆盖行级分流、表级阻断、告警触发

- [x] Task 8: sparketl-monitor —— 可观测能力（R6）
  - [x] SubTask 8.1: SLF4J/Logback 标准化日志（统一 pattern、敏感字段脱敏 Layout），移除所有裸 println
  - [x] SubTask 8.2: JobRunStore：JDBC 持久化运行记录（作业名/批次/状态/起止时间/输入输出行数/错误信息），附状态库初始化 SQL
  - [x] SubTask 8.3: Spark Listener 指标采集（阶段耗时、输入输出行数、任务失败数）+ Dropwizard Metrics（SLF4J Reporter + Prometheus Sink）
  - [x] 验证：local 作业运行后运行记录落库（H2 单测验证）、指标经 /metrics 端点可抓取（PrometheusMetricsSpec 验证）

- [x] Task 9: 全链路集成与扩展示例（R8、R9 部分）
  - [x] SubTask 9.1: 打通 CLI 入口 `SparkEtlEntrypoint`（--config job.yaml）串联生命周期 + 质量 + 监控 + 重试
  - [x] SubTask 9.2: sparketl-examples：YAML 示例（jdbc→hive、hive→jdbc、csv→iceberg、kafka→delta、local_range_demo）、自定义 Transformer（迁移旧 GPS 清洗逻辑）、自定义 DataSourceProvider、自定义 QualityRule 各一个
  - [x] 验证：示例在 local 模式可运行通过（local_range_demo 端到端单测 + 本地 spark-submit 验证）

- [x] Task 10: 文档与发布（R9、R10）
  - [x] SubTask 10.1: README（定位/架构图/特性/模块说明）、快速开始、YAML 配置参考（全字段）、部署指南（YARN 提交、状态库初始化、Prometheus/Grafana 对接）
  - [x] SubTask 10.2: 全代码 ScalaDoc 注释补齐，`mvn clean package` 最终验证
  - [x] 验证：按文档从零提交一个示例作业可成功运行

# Task Dependencies
- Task 2、3 依赖 Task 1（工程骨架）
- Task 4、5 依赖 Task 2、3（配置模型与 SparkSession）
- Task 6 可与 Task 4 并行（仅依赖 Task 2）
- Task 7 依赖 Task 5（transform 输出作为校验输入）
- Task 8 依赖 Task 5（生命周期埋点）
- Task 9 依赖 Task 4~8（全模块串联）
- Task 10 依赖 Task 9（文档需与最终实现一致）
