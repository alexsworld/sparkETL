# Spark 3.5.x 企业级大数据 ETL 框架重构 Spec

## Why
现有代码（sparketl.md / sparketl2.md）为 2019 年前后的遗留实现：Spark 2.x 时代 API、硬编码的数据库连接与明文密码、按 `task_name` 硬编码 match 分发任务、`System.exit` 暴力退出、`println` 式日志、无重试/幂等/数据质量/监控能力，无法利用 Spark 3.5.x 的 AQE、动态分区裁剪、向量化引擎等优化。需要将其重构为一套开源的、配置驱动、可直接用于生产环境的基于 Spark 3.5.x 的企业级大数据 ETL 框架。

## What Changes
- **BREAKING** 全面废弃原有代码结构与运行方式（原 `com.sutpc.bigdata.*` 包、`EtlApp` 按 task_name match 分发、`etl_job_config`/`etl_job_log` MySQL 配置表模型），重构为全新 Maven 多模块开源项目 `sparketl`；旧代码仅作为业务逻辑参考，不做兼容保留。
- 升级基座：Spark 3.5.x + Scala 2.12 + Java 8/17 双兼容，默认启用 AQE（`spark.sql.adaptive.*`）、动态分区裁剪（DPF）、Parquet/ORC 向量化读取，Kryo 序列化注册。
- 实现标准 ETL 作业生命周期：`配置解析 → 环境校验 → 资源初始化 → Extract → Transform → Load → 质量校验 → 状态记录 → 资源清理`，以 `JobLifecycle` 状态机统一封装。
- 内置统一连接器层（`Reader`/`Writer` SPI）：JDBC、Hive、HDFS（Parquet/ORC/CSV/JSON/Text）、S3（S3A）、Kafka、Iceberg、Delta Lake，统一声明式读写接口与连接参数模型。
- 提供声明式配置驱动开发模式：单个作业仅需一份 YAML 配置（含 input/output/transform/quality/retry/monitor 段）即可运行，无需编写 Spark 基础代码。
- 内置数据质量模块：行级表达式规则（null/范围/正则/自定义 SQL 表达式）、表级规则（行数阈值、唯一性、聚合比对），异常数据自动分流至隔离区（quarantine 路径/表）并触发告警。
- 完善可观测性：SLF4J/Logback 标准化日志、作业运行状态追踪（JobRunStore：JDBC 持久化运行记录）、运行指标采集（Spark Listener 收集输入/输出行数、耗时、分区数，对接 Dropwizard Metrics + Prometheus Sink），预留 Grafana 可视化指标。
- 容错与重试：针对连接异常、任务失败提供可配置重试策略（固定间隔/指数退避 + 抖动），写入侧通过"临时 staging → 提交"模式与分区覆盖幂等保障，失败自动回滚清理。
- 标准化扩展接口：`Transformer`（自定义转换）、`DataSourceProvider`（自定义数据源）、`QualityRule`（自定义校验规则）三类 SPI，通过 Java `ServiceLoader` + 配置注册无缝接入。
- 完整使用文档（README、快速开始、配置参考、部署指南）、示例工程（sparketl-examples 含典型 YAML 作业与自定义扩展示例）。
- 遵循阿里巴巴 Java/Scala 开发规范：分层清晰、注释完整（ScalaDoc）、统一常量管理，Maven 一键构建（`mvn clean package`）产出可发布构件。

## Impact
- Affected specs: 无（首个变更）
- Affected code:
  - 新建 Maven 多模块工程：`sparketl-core`（生命周期/配置/重试/指标核心）、`sparketl-connectors`（连接器）、`sparketl-quality`（数据质量）、`sparketl-monitor`（可观测）、`sparketl-examples`（示例）、`sparketl-dist`（assembly 打包）
  - 参考重构的旧逻辑（来自 sparketl.md / sparketl2.md）：`EtlApp`、`TaskParser/TaskRegister`（改为配置驱动注册表）、`TaskOutput`（改为 Writer SPI + 幂等提交）、`DB2HivePartitionExecutor` 等（改为通用 JDBC→Hive 连接器作业）、GPS 系列 Executor（作为 sparketl-examples 中的自定义 Transformer 示例）、`HdfsStatJob`（作为 HDFS 监控示例）

## ADDED Requirements

### Requirement: R1 Spark 3.5.x 基座与性能优化
系统 SHALL 基于 Spark 3.5.x 构建，并提供默认开启的现代化性能配置。

#### Scenario: AQE 与向量化引擎默认生效
- **WHEN** 用户通过框架启动任意作业且未显式覆盖
- **THEN** `spark.sql.adaptive.enabled=true`、`spark.sql.adaptive.coalescePartitions.enabled=true`、`spark.sql.adaptive.skewJoin.enabled=true`、动态分区裁剪（`spark.sql.optimizer.excludedRules` 不排除 DPF）、Parquet/ORC 向量化读取均为开启状态，且 KryoSerializer 已配置
- **AND** 用户可通过作业配置覆盖任意 SparkConf，覆盖值优先级高于默认值

#### Scenario: 部署环境自适应
- **WHEN** 作业分别以 local 与 yarn cluster 模式提交
- **THEN** 框架均能正常初始化 SparkSession，Hive 支持可按配置开关，无需修改任何代码

### Requirement: R2 ETL 作业生命周期管理
系统 SHALL 提供标准化的作业生命周期封装，覆盖配置解析、资源初始化、数据抽取、转换、加载、异常重试、资源清理全流程。

#### Scenario: 作业正常执行
- **WHEN** 用户提交一份合法的作业 YAML 配置
- **THEN** 框架按序执行：解析并校验配置 → 初始化 SparkSession/广播资源 → Extract → Transform 链 → Load（幂等提交）→ 质量校验 → 写运行记录 → 清理临时资源，全程生命周期事件有日志与指标埋点

#### Scenario: 配置非法时快速失败
- **WHEN** 配置缺失必填字段或类型不合法
- **THEN** 在提交到集群前即完成配置校验并以明确的错误信息退出（非 `System.exit`，通过异常 + 非零退出码），不产生任何集群资源开销

#### Scenario: 失败清理
- **WHEN** 作业在任意阶段抛出异常且重试耗尽
- **THEN** 已写入的 staging 临时数据被清理，运行记录标记为 FAILED 并保留异常堆栈，SparkSession 正常 stop，进程以非零码退出

### Requirement: R3 统一数据源连接器
系统 SHALL 内置 JDBC、Hive、HDFS、S3、Kafka、Iceberg、Delta Lake 的统一读写连接器，全部实现统一的 `Reader`/`Writer` SPI。

#### Scenario: 声明式读写
- **WHEN** 用户在 YAML 中声明 `input.type: jdbc`（含 url/user/password/driver/table/query/options）与 `output.type: iceberg`
- **THEN** 框架自动路由到对应连接器完成读取与写入，无需编写任何 Spark 代码

#### Scenario: 连接参数统一建模
- **WHEN** 不同连接器配置连接参数
- **THEN** 敏感项（password、token、secretKey）支持 `${env:VAR}` 环境变量占位符注入，日志与运行记录中不得出现明文敏感值

#### Scenario: JDBC 读取并发优化
- **WHEN** JDBC 输入配置了分区列与分片数（partitionColumn/lowerBound/upperBound/numPartitions 或 predicates）
- **THEN** 框架按分片并行读取，未配置时给出性能提示日志

### Requirement: R4 声明式配置驱动开发
系统 SHALL 支持仅通过 YAML 配置文件完成常规 ETL 作业开发。

#### Scenario: 零代码作业
- **WHEN** 用户编写一份包含 `input/output/transform/retry/quality/monitor` 段的 YAML 并提交
- **THEN** 作业可完整运行，内置 transform 库（SQL 表达式、字段选择/重命名/类型转换、去重、过滤、行级清洗）覆盖常规需求

#### Scenario: 多目标输出
- **WHEN** 用户在 output 段声明多个目标（如同时写 Hive 与 JDBC）
- **THEN** 框架对多目标复用同一转换结果，避免重复计算

### Requirement: R5 数据质量校验
系统 SHALL 内置数据质量模块，支持行级、表级自定义规则校验、异常数据自动分流与告警。

#### Scenario: 行级规则分流
- **WHEN** 配置了行级规则（如 `lng between 73 and 136`）且部分数据违规
- **THEN** 违规数据被分流写入隔离区（配置的 quarantine 路径/表，附带规则名与批次时间列），合格数据正常加载

#### Scenario: 表级规则阻断
- **WHEN** 配置了表级规则（如最低行数阈值、主键唯一性）且校验失败
- **THEN** 作业按策略（FAIL 阻断 / WARN 继续）执行；FAIL 时终止加载并记录运行失败原因

#### Scenario: 告警
- **WHEN** 任何质量规则违规或分流发生
- **THEN** 按配置的告警通道（日志 / HTTP Webhook）发出告警通知

### Requirement: R6 监控与可观测
系统 SHALL 提供指标采集、标准化日志、作业状态追踪与运行指标可视化支持。

#### Scenario: 作业状态追踪
- **WHEN** 作业启动、成功、失败
- **THEN** `JobRunStore` 在 JDBC 状态库中持久化运行记录（作业名、批次时间、状态、开始/结束时间、输入/输出行数、错误信息），旧版 `etl_job_log` 能力被此取代

#### Scenario: 指标采集
- **WHEN** 作业运行
- **THEN** Spark Listener 采集各阶段耗时、输入/输出行数、任务失败数等指标，通过 Dropwizard Metrics 输出至 SLF4J 与 Prometheus Sink，可直接对接 Grafana

#### Scenario: 日志标准化
- **WHEN** 作业运行
- **THEN** 全部输出走 SLF4J/Logback 结构化日志（统一格式：时间/级别/作业名/阶段），无任何裸 `println`，敏感字段脱敏

### Requirement: R7 容错与重试
系统 SHALL 针对数据源连接异常、任务执行失败提供可配置重试策略与幂等性保障。

#### Scenario: 连接重试
- **WHEN** 数据源初始化/读取时抛出连接类异常
- **THEN** 按配置策略（fixed/exponential-backoff，含最大次数与抖动）自动重试，重试期间记录 WARN 日志

#### Scenario: 写入幂等
- **WHEN** 作业失败后重新运行同一批次
- **THEN** 写入通过 staging 目录 + 原子提交（或 Iceberg/Delta 快照、JDBC upsert、分区覆盖）保证目标表不产生重复数据

#### Scenario: 重试耗尽
- **WHEN** 达到最大重试次数仍失败
- **THEN** 清理 staging，作业标记 FAILED，异常信息完整保留在运行记录中

### Requirement: R8 标准化扩展接口
系统 SHALL 提供自定义转换、自定义数据源、自定义校验规则三类扩展 SPI。

#### Scenario: 自定义转换
- **WHEN** 用户实现 `Transformer` 接口并通过 ServiceLoader 或 YAML `transformers` 段注册
- **THEN** 该转换按声明顺序加入作业转换链执行

#### Scenario: 自定义数据源
- **WHEN** 用户实现 `DataSourceProvider` 并注册（`type: custom-xxx`）
- **THEN** YAML 中即可像内置连接器一样声明使用

#### Scenario: 自定义质量规则
- **WHEN** 用户实现 `QualityRule` 并注册
- **THEN** 该规则可在 quality 段按名称引用，与内置规则同等分流/阻断/告警

### Requirement: R9 文档、示例与开箱即用
系统 SHALL 提供完整使用文档、示例代码与部署指南，确保开箱即用的生产可用性。

#### Scenario: 文档完整
- **WHEN** 新用户获取项目
- **THEN** README（项目定位/架构图/特性）、快速开始、YAML 配置参考（全字段说明）、部署指南（local/YARN 提交、依赖部署、状态库初始化 SQL、Prometheus/Grafana 对接）齐备，sparketl-examples 含可运行的典型作业与自定义扩展示例

### Requirement: R10 开源工程规范
系统 SHALL 遵循开源项目规范与阿里巴巴 Java/Scala 开发规范，支持 Maven 一键构建。

#### Scenario: 一键构建
- **WHEN** 执行 `mvn clean package`
- **THEN** 全模块编译通过，sparketl-dist 产出包含全部依赖的 assembly jar；scalafmt/spotless 检查通过，代码注释完整
