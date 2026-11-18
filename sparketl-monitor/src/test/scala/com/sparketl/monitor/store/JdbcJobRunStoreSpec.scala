package com.sparketl.monitor.store

import java.sql.DriverManager
import java.time.LocalDateTime

import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import com.sparketl.core.spi.{JobRunRecord, JobStatus}

/**
 * [[JdbcJobRunStore]] 测试：基于 H2 内存库验证完整生命周期与幂等语义。
 */
class JdbcJobRunStoreSpec extends AnyFunSuite with BeforeAndAfterEach {

  /** H2 内存库（DB_CLOSE_DELAY=-1 保证连接关闭后数据保留）。 */
  private val Url = "jdbc:h2:mem:runstore;DB_CLOSE_DELAY=-1"
  private val Table = JdbcJobRunStore.DEFAULT_TABLE

  /** 查询结果行（断言用）。 */
  private case class Row(
      status: String,
      startTime: LocalDateTime,
      inputRows: Long,
      outputRows: Long,
      quarantinedRows: Long,
      errorMessage: Option[String],
      endTime: Option[LocalDateTime])

  override def beforeEach(): Unit = {
    // 每个用例重建表，保证隔离
    val conn = DriverManager.getConnection(Url)
    try {
      val stmt = conn.createStatement()
      try stmt.executeUpdate(s"DROP TABLE IF EXISTS $Table")
      finally stmt.close()
    } finally conn.close()
    new JdbcJobRunStore(Url, None, None, Table)
    ()
  }

  private def newStore(): JdbcJobRunStore = new JdbcJobRunStore(Url, None, None, Table)

  private def queryRow(runId: String): Option[Row] = {
    val conn = DriverManager.getConnection(Url)
    try {
      val ps = conn.prepareStatement(
        s"SELECT status, start_time, input_rows, output_rows, quarantined_rows, error_message, end_time FROM $Table WHERE run_id = ?")
      try {
        ps.setString(1, runId)
        val rs = ps.executeQuery()
        try {
          if (rs.next()) Some(Row(
            rs.getString("status"),
            rs.getTimestamp("start_time").toLocalDateTime,
            rs.getLong("input_rows"),
            rs.getLong("output_rows"),
            rs.getLong("quarantined_rows"),
            Option(rs.getString("error_message")),
            Option(rs.getTimestamp("end_time")).map(_.toLocalDateTime)))
          else None
        } finally rs.close()
      } finally ps.close()
    } finally conn.close()
  }

  private def countRows(): Int = {
    val conn = DriverManager.getConnection(Url)
    try {
      val stmt = conn.createStatement()
      try {
        val rs = stmt.executeQuery(s"SELECT COUNT(*) FROM $Table")
        try {
          rs.next()
          rs.getInt(1)
        } finally rs.close()
      } finally stmt.close()
    } finally conn.close()
  }

  private def baseRecord(runId: String): JobRunRecord = {
    val start = LocalDateTime.of(2026, 1, 1, 8, 0, 0)
    JobRunRecord(runId = runId, jobName = "demoJob", batchTime = "2026-01-01 08:00:00",
      status = JobStatus.Running, startTime = start)
  }

  test("start 后写入 RUNNING 记录，success 后更新为 SUCCEEDED 且行数正确") {
    val store = newStore()
    val record = baseRecord("run-success")
    val endTime = record.startTime.plusMinutes(5)

    store.start(record)
    val running = queryRow("run-success")
    assert(running.isDefined, "start 后应存在记录")
    assert(running.get.status == JobStatus.Running)
    assert(running.get.endTime.isEmpty)

    store.success(record.copy(
      status = JobStatus.Succeeded,
      endTime = Some(endTime),
      inputRows = 100L, outputRows = 90L, quarantinedRows = 10L))

    val row = queryRow("run-success")
    assert(row.isDefined)
    assert(row.get.status == JobStatus.Succeeded)
    assert(row.get.inputRows == 100L)
    assert(row.get.outputRows == 90L)
    assert(row.get.quarantinedRows == 10L)
    assert(row.get.endTime.contains(endTime))
    assert(countRows() == 1, "success 是 UPDATE 语义，不应新增行")
  }

  test("failure 后更新为 FAILED 并写入截断后的 error_message") {
    val store = newStore()
    val record = baseRecord("run-failed")
    val endTime = record.startTime.plusMinutes(1)
    val longError = "数据库连接超时: " + "x" * 6000

    store.start(record)
    store.failure(record.copy(
      status = JobStatus.Failed, endTime = Some(endTime), errorMessage = Some(longError)))

    val row = queryRow("run-failed")
    assert(row.isDefined)
    assert(row.get.status == JobStatus.Failed)
    assert(row.get.errorMessage.exists(_.nonEmpty))
    assert(row.get.errorMessage.get.length <= 4000, "error_message 必须截断到 4000 字符内")
    assert(row.get.endTime.contains(endTime))
  }

  test("同一 runId 重复 start 不产生重复行（幂等）") {
    val store = newStore()
    val record = baseRecord("run-dup")

    store.start(record)
    store.start(record.copy(startTime = record.startTime.plusSeconds(30)))

    assert(countRows() == 1, "重复 start 应先 DELETE 后 INSERT，不产生重复行")
    val row = queryRow("run-dup")
    assert(row.isDefined)
    assert(row.get.status == JobStatus.Running)
    assert(row.get.endTime.isEmpty, "start 阶段不写结束时间")
    assert(row.get.startTime == LocalDateTime.of(2026, 1, 1, 8, 0, 30),
      "应保留第二次 start 的内容")
  }
}
