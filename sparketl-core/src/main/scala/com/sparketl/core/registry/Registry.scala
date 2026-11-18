package com.sparketl.core.registry

import java.util.ServiceLoader

import scala.collection.JavaConverters._

import org.slf4j.LoggerFactory

import com.sparketl.core.config.ConfigException
import com.sparketl.core.spi.{DataSourceProvider, Transformer}

/**
 * SPI 注册表：统一加载与路由 DataSourceProvider / Transformer。
 *
 * 加载机制为 Java ServiceLoader：
 *  - 连接器：META-INF/services/com.sparketl.core.spi.DataSourceProvider
 *  - 自定义转换器：META-INF/services/com.sparketl.core.spi.Transformer
 */
object Registry {

  private val log = LoggerFactory.getLogger(getClass)

  /** 已注册连接器：type -> provider。 */
  private lazy val providers: Map[String, DataSourceProvider] = {
    val loaded = ServiceLoader.load(classOf[DataSourceProvider]).asScala
      .map(p => p.providerType -> p).toMap
    log.info("已加载数据源连接器: {}", loaded.keys.mkString(", "))
    loaded
  }

  /** 已注册自定义转换器：name -> transformer（内置转换不在此处）。 */
  private lazy val customTransformers: Map[String, Transformer] = {
    val loaded = ServiceLoader.load(classOf[Transformer]).asScala
      .map(t => t.name -> t).toMap
    if (loaded.nonEmpty) {
      log.info("已加载自定义转换器: {}", loaded.keys.mkString(", "))
    }
    loaded
  }

  /** 当前可用连接器类型集合（用于诊断信息输出）。 */
  def registeredTypes: Set[String] = providers.keySet

  /** 按类型获取连接器，未注册时抛出 [[ConfigException]]。 */
  def provider(dsType: String): DataSourceProvider =
    providers.getOrElse(dsType, throw new ConfigException(
      s"未注册的数据源类型 '$dsType'，当前可用: ${registeredTypes.mkString(", ")}"))

  /** 按类型获取自定义转换器，未注册时抛出 [[ConfigException]]。 */
  def customTransformer(name: String): Transformer =
    customTransformers.getOrElse(name, throw new ConfigException(
      s"未注册的自定义转换器 '$name'，当前可用: ${customTransformers.keys.mkString(", ")}"))
}
