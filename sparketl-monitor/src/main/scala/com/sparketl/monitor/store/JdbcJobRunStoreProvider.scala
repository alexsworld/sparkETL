package com.sparketl.monitor.store

import com.sparketl.core.spi.{JobRunStore, JobRunStoreProvider, RunStoreConfigHolder}

/**
 * JDBC 运行状态存储的 SPI 工厂实现。
 *
 * 通过 Java ServiceLoader 注册（见 META-INF/services），
 * 对应 YAML 配置 `monitor.runStore` 段，`storeType` 固定为 `jdbc`。
 */
class JdbcJobRunStoreProvider extends JobRunStoreProvider {

  /** 存储类型标识。 */
  override def storeType: String = "jdbc"

  /** 根据配置创建 JDBC 存储，表名缺省使用 [[JdbcJobRunStore.DEFAULT_TABLE]]。 */
  override def create(config: RunStoreConfigHolder): JobRunStore =
    new JdbcJobRunStore(
      url = config.url,
      user = config.user,
      password = config.password,
      table = config.table.getOrElse(JdbcJobRunStore.DEFAULT_TABLE))
}
