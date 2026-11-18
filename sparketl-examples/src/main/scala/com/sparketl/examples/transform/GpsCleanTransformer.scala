package com.sparketl.examples.transform

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import com.sparketl.core.spi.Transformer

/**
 * 自定义转换器示例：GPS 定位数据清洗（由遗留硬编码 ETL 逻辑迁移而来）。
 *
 * 清洗规则：
 *  1. 过滤经纬度非法（超出全球范围 / 全零坐标）的记录；
 *  2. 按设备 + 定位时间去重，保留首条；
 *  3. 追加批次时间列 etl_batch_time。
 *
 * 注册方式（ServiceLoader）：
 * `META-INF/services/com.sparketl.core.spi.Transformer` 声明本类全限定名，
 * YAML 中引用：
 * {{{
 * transforms:
 *   - kind: custom
 *     name: gps_clean
 *     params: { lonCol: "lon", latCol: "lat" }
 * }}}
 *
 * @note 参数均为可选，缺省列名：lon / lat / device_id / gps_time。
 */
class GpsCleanTransformer extends Transformer {

  import GpsCleanTransformer._

  /** SPI 注册名，与 YAML `transforms[].name` 对应。 */
  override def name: String = "gps_clean"

  override def transform(spark: SparkSession, df: DataFrame, params: Map[String, String]): DataFrame = {
    val lon = params.getOrElse(LonColKey, DefaultLonCol)
    val lat = params.getOrElse(LatColKey, DefaultLatCol)
    val device = params.getOrElse(DeviceColKey, DefaultDeviceCol)
    val timeCol = params.getOrElse(TimeColKey, DefaultTimeCol)
    val batchTime = params.getOrElse(BatchTimeKey, "")

    val validCoords = df.filter(
      col(lon).isNotNull && col(lat).isNotNull &&
        col(lon).between(MinLon, MaxLon) &&
        col(lat).between(MinLat, MaxLat) &&
        col(lon) =!= 0.0 && col(lat) =!= 0.0)

    val deduped = validCoords.dropDuplicates(Seq(device, timeCol))

    if (batchTime.nonEmpty) deduped.withColumn("etl_batch_time", lit(batchTime))
    else deduped
  }
}

object GpsCleanTransformer {
  val LonColKey     = "lonCol"
  val LatColKey     = "latCol"
  val DeviceColKey  = "deviceCol"
  val TimeColKey    = "timeCol"
  val BatchTimeKey  = "batchTime"

  val DefaultLonCol    = "lon"
  val DefaultLatCol    = "lat"
  val DefaultDeviceCol = "device_id"
  val DefaultTimeCol   = "gps_time"

  val MinLon = -180.0
  val MaxLon = 180.0
  val MinLat = -90.0
  val MaxLat = 90.0
}
