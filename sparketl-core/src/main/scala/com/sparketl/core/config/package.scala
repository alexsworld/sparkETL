package com.sparketl.core.config

/**
 * ETL 作业配置模型（与 YAML 配置文件一一对应，YAML 键统一使用 snake_case）。
 *
 * 典型 YAML 结构示例：
 * {{{
 * name: gps_clean_job
 * batchTime: "2026-11-18 10:00:00"
 * spark:
 *   spark.master: local[*]
 * input:
 *   type: jdbc
 *   options: { url: ..., table: ... }
 * outputs:
 *   - type: hive
 *     mode: overwrite
 *     options: { table: ods.gps_clean }
 * transforms:
 *   - kind: filter
 *     params: { condition: "lon BETWEEN -180 AND 180" }
 * quality:
 *   rowRules: [{ name: not_null, expression: "device_id IS NOT NULL" }]
 *   tableRules: [{ name: row_count, kind: minRows, params: { minRows: "1" } }]
 *   onFailure: WARN
 * retry:
 *   maxAttempts: 3
 *   policy: exponential
 * monitor:
 *   runStore: { url: ..., table: sparketl_run_store }
 * }}}
 */
package object config
