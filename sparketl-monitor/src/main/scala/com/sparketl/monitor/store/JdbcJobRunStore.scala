package com.sparketl.monitor.store

import java.sql.{Connection, DriverManager, PreparedStatement, Statement, Timestamp}
import java.time.LocalDateTime

import scala.util.control.NonFatal

import org.slf4j.LoggerFactory

import com.sparketl.core.spi.{JobRunRecord, JobRunStore, JobStatus}

/**
 * 基于 JDBC 的作业运行状态存储实现（生产默认实现）。
 *
 * 设计要点：
 *  - 仅依赖纯 java.sql（DriverManager / PreparedStatement），不依赖 Spark 运行时；
 *  - 表结构按需自动创建（CREATE TABLE IF NOT EXISTS），兼容 H2 / MySQL / PostgreSQL 等；
 *  - start 采用「先 DELETE 后 INSERT」的幂等写入，同一 runId 重复 start 不产生重复行；
 *  - success / failure 采用按 runId 的 UPDATE 回填终态；
 *  - 状态库故障不应导致 ETL 作业失败：所有 SQL 异常捕获后仅记录 error 日志（URL 已脱敏，
 *    密码不落日志），方法本身不向生命周期抛出异常；
 *  - error_message 写库前截断到 4000 字符以内，避免超出列宽引发二次失败。
 *
 * @param url      JDBC 连接串（可含 user/password 参数，日志输出时自动脱敏）
 * @param user     可选用户名
 * @param password 可选密码（禁止打印到日志）
 * @param table    运行记录表名（缺省 [[JdbcJobRunStore.DEFAULT_TABLE]]）
 */
class JdbcJobRunStore(url: String, user: Option[String], password: Option[String], table: String)
    extends JobRunStore {

  private val log = LoggerFactory.getLogger(classOf[JdbcJobRunStore])

  /** error_message 列宽上限（字符数）。 */
  private val MaxErrorLength = 4000

  /** 匹配 URL 中的密码参数（用于脱敏），不区分大小写。 */
  private val SensitiveUrlParam = "(?i)(password|pwd)=[^;&]*".r

  /** 全列插入语句（占位符顺序与 bindInsert 一致）。 */
  private val InsertSql =
    s"INSERT INTO $table (run_id, job_name, batch_time, status, start_time, end_time, " +
      "input_rows, output_rows, quarantined_rows, error_message) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

  /** 终态回填语句（success / failure 共用）。 */
  private val UpdateSql =
    s"UPDATE $table SET status = ?, end_time = ?, input_rows = ?, output_rows = ?, " +
      "quarantined_rows = ?, error_message = ? WHERE run_id = ?"

  // 实例创建时即确保表存在；失败不阻断构造（后续操作会继续尝试）。
  ensureTable()

  /** 记录作业启动：先删除同 runId 旧行再插入，保证幂等，状态固定为 RUNNING。 */
  override def start(record: JobRunRecord): Unit = withConnection { conn =>
    execUpdate(conn, s"DELETE FROM $table WHERE run_id = ?") { ps =>
      ps.setString(1, record.runId)
    }
    execUpdate(conn, InsertSql) { ps =>
      ps.setString(1, record.runId)
      ps.setString(2, record.jobName)
      ps.setString(3, record.batchTime)
      ps.setString(4, JobStatus.Running)
      ps.setTimestamp(5, Timestamp.valueOf(record.startTime))
      ps.setTimestamp(6, toTimestamp(record.endTime))
      ps.setLong(7, record.inputRows)
      ps.setLong(8, record.outputRows)
      ps.setLong(9, record.quarantinedRows)
      ps.setString(10, truncateError(record.errorMessage))
    }
    ()
  }

  /** 记录作业成功：按 runId 回填 SUCCEEDED 终态。 */
  override def success(record: JobRunRecord): Unit = finish(record, JobStatus.Succeeded)

  /** 记录作业失败：按 runId 回填 FAILED 终态与错误信息。 */
  override def failure(record: JobRunRecord): Unit = finish(record, JobStatus.Failed)

  private def finish(record: JobRunRecord, status: String): Unit = withConnection { conn =>
    execUpdate(conn, UpdateSql) { ps =>
      ps.setString(1, status)
      ps.setTimestamp(2, toTimestamp(record.endTime))
      ps.setLong(3, record.inputRows)
      ps.setLong(4, record.outputRows)
      ps.setLong(5, record.quarantinedRows)
      ps.setString(6, truncateError(record.errorMessage))
      ps.setString(7, record.runId)
    }
    ()
  }

  // ------------------------------------------------------------------
  // 内部工具
  // ------------------------------------------------------------------

  /** 创建运行记录表（幂等）。 */
  private def ensureTable(): Unit = withConnection { conn =>
    var stmt: Statement = null
    try {
      stmt = conn.createStatement()
      stmt.execute(
        s"CREATE TABLE IF NOT EXISTS $table (" +
          "run_id VARCHAR(64) PRIMARY KEY, " +
          "job_name VARCHAR(255) NOT NULL, " +
          "batch_time VARCHAR(64), " +
          "status VARCHAR(16) NOT NULL, " +
          "start_time TIMESTAMP, " +
          "end_time TIMESTAMP, " +
          "input_rows BIGINT, " +
          "output_rows BIGINT, " +
          "quarantined_rows BIGINT, " +
          "error_message VARCHAR(4000))")
      ()
    } finally {
      if (stmt != null) try stmt.close() catch { case NonFatal(e) =>
        log.warn(s"关闭 Statement 失败: ${describe(e)}")
      }
    }
  }

  /**
   * 统一的连接使用模板：获取连接执行回调，SQL 异常捕获后仅记录脱敏日志并吞掉
   * （状态库故障不应导致作业失败），连接在 finally 中保证关闭。
   */
  private def withConnection[A](body: Connection => A): Unit = {
    var conn: Connection = null
    try {
      conn = openConnection()
      body(conn)
      ()
    } catch {
      case NonFatal(e) =>
        log.error(s"运行状态库操作失败（不影响作业执行）: url=${safeUrl(url)}, table=$table, error=${describe(e)}")
    } finally {
      if (conn != null) try conn.close() catch { case NonFatal(e) =>
        log.warn(s"关闭运行状态库连接失败: ${describe(e)}")
      }
    }
  }

  /** 执行带占位符绑定的更新语句，Statement 在 finally 中关闭。 */
  private def execUpdate(conn: Connection, sql: String)(bind: PreparedStatement => Unit): Int = {
    var ps: PreparedStatement = null
    try {
      ps = conn.prepareStatement(sql)
      bind(ps)
      ps.executeUpdate()
    } finally {
      if (ps != null) ps.close()
    }
  }

  private def openConnection(): Connection = user match {
    case Some(u) => DriverManager.getConnection(url, u, password.getOrElse(""))
    case None    => DriverManager.getConnection(url)
  }

  /** LocalDateTime -> java.sql.Timestamp，None 映射为 null。 */
  private def toTimestamp(time: Option[LocalDateTime]): Timestamp =
    time.map(Timestamp.valueOf).orNull

  /** 错误信息截断到 4000 字符以内（与 error_message 列宽对齐），None 映射为 null。 */
  private def truncateError(message: Option[String]): String = message match {
    case None => null
    case Some(m) =>
      if (m == null || m.isEmpty) null
      else if (m.length > MaxErrorLength) m.substring(0, MaxErrorLength)
      else m
  }

  /** 对 JDBC URL 中的密码参数脱敏，用于日志输出。 */
  private def safeUrl(rawUrl: String): String =
    SensitiveUrlParam.replaceAllIn(rawUrl, m => s"${m.group(1)}=***")

  /** 汇总异常类名与消息，避免日志 API 携带异常对象。 */
  private def describe(e: Throwable): String = s"${e.getClass.getName}: ${e.getMessage}"
}

/** [[JdbcJobRunStore]] 常量与工具。 */
object JdbcJobRunStore {

  /** 默认运行记录表名。 */
  final val DEFAULT_TABLE = "sparketl_run_store"
}
