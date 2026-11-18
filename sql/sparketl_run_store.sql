-- SparkETL 运行状态库初始化脚本
-- 用途：持久化作业运行记录，支撑作业状态追踪与可视化（Grafana / Metabase 等）
-- 兼容：MySQL 5.7+ / PostgreSQL 9.5+ / H2 / 其他主流关系库
-- 说明：run_id 使用 VARCHAR(64) 主键（框架生成 jobName-时间戳-随机后缀），避免自增方言差异

CREATE TABLE IF NOT EXISTS sparketl_run_store (
    run_id           VARCHAR(64)  PRIMARY KEY COMMENT '运行唯一标识 (jobName-时间戳-随机后缀)',
    job_name         VARCHAR(255) NOT NULL                COMMENT '作业名称',
    batch_time       VARCHAR(64)                          COMMENT '批次时间',
    status           VARCHAR(16)  NOT NULL                COMMENT '状态: RUNNING | SUCCEEDED | FAILED',
    start_time       TIMESTAMP                            COMMENT '开始时间',
    end_time         TIMESTAMP                            COMMENT '结束时间',
    input_rows       BIGINT                               COMMENT '输入行数',
    output_rows      BIGINT                               COMMENT '输出行数',
    quarantined_rows BIGINT                               COMMENT '质量隔离行数',
    error_message    VARCHAR(4000)                        COMMENT '失败信息（已脱敏，超长截断）'
);

-- 常用查询索引（MySQL 语法；PostgreSQL 同样支持）
CREATE INDEX idx_run_store_job_time ON sparketl_run_store (job_name, start_time);
CREATE INDEX idx_run_store_status   ON sparketl_run_store (status);

-- 常用监控 SQL 示例 ------------------------------------------------------------
-- 最近 24 小时失败作业：
--   SELECT * FROM sparketl_run_store
--   WHERE status = 'FAILED' AND start_time > NOW() - INTERVAL 24 HOUR
--   ORDER BY start_time DESC;
--
-- 每日作业成功率：
--   SELECT DATE(start_time) AS dt,
--          COUNT(*) AS total,
--          SUM(CASE WHEN status = 'SUCCEEDED' THEN 1 ELSE 0 END) AS ok,
--          ROUND(SUM(CASE WHEN status = 'SUCCEEDED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS ok_rate
--   FROM sparketl_run_store GROUP BY DATE(start_time) ORDER BY dt DESC;
