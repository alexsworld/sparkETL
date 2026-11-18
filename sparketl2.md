@[TOC](目录)

## 数据同步 RDMBS to RDMBS

```scala
package com.sutpc.bigdata.sync

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * <p>Description:TODO  </p>
  * <p>Copyright: Copyright (c) 2019</p>
  * <p>Company: 深圳市城市交通规划研究中心</p>
  *
  * @author zhangyongtian
  * @version 1.0.0
  *          date 2019/10/18  9:50
  */
object DataSync {

  def main(args: Array[String]): Unit = {

    val app = s"${this.getClass.getSimpleName}".filter(!_.equals('$'))

    Logger.getLogger("org").setLevel(Level.ERROR)


    val spark: SparkSession = SparkSession.builder
      .appName(app)
      .master("local[*]")
      .config("spark.shuffle.consolidateFiles", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.streaming.kafka.maxRatePerPartition", "500")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.network.timeout", "600")
      .config("spark.streaming.kafka.consumer.poll.ms", "60000")
      .config("spark.core.connection.ack.wait.timeout", "900")
      .config("spark.rpc.message.maxSize", "50")
      .config("spark.akka.timeout", "900")
      .getOrCreate()

    //数据源
    val transpass_url = s"jdbc:postgresql://10.10.201.28:5432/postgres"
    val transpass_prop = new Properties()
    transpass_prop.put("user", "postgres")
    transpass_prop.put("password", "123456")
    transpass_prop.put("driver", "org.postgresql.Driver")

    //目标 5432
    val key_url = s"jdbc:postgresql://10.10.201.50:54320/vehicle"
    val key_prop = new Properties()
    key_prop.put("user", "postgres")
    key_prop.put("password", "123456")
    key_prop.put("driver", "org.postgresql.Driver")

    import spark.implicits._
    spark.read.jdbc(transpass_url, "public.huangpu_road", transpass_prop).write.jdbc(key_url, "public.huangpu_road", key_prop)


  }

}

```

## 数据同步 Hive to Hive 

```scala
object SparkHive2Hive {

  def main(args: Array[String]): Unit = {
    val startTime = DateTime.now()

    val app = s"${this.getClass.getSimpleName}".filter(!_.equals('$'))

    Logger.getLogger("org").setLevel(Level.ERROR)


    //TODO:spark初始化
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(app)
      .config("spark.sql.parquet.writeLegacyFormat", true)
      //      .enableHiveSupport()
      .getOrCreate()

    //      .write.json("/import/transpaas_data/t_phone_home_distribute_json")

    //hive强制分桶
    //    spark.sql("set hive.enforce.bucketing=true")
    //    spark.sqlContext.setConf("spark.sql.hive.convertMetastoreParquet", "false")
    //    spark.read.json("/import/transpaas_data/t_phone_home_distribute_json")


    //开启动态分区
    //    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("set spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict")
    spark.read.parquet("/import/transpaas_data/t_phone_home_distribute")
      .write
      .mode("append")
      .format("Hive")
      .partitionBy("source", "city", "year", "month", "day")
      .saveAsTable("transpaas_tag.t_phone_home_distribute")

    spark.sql("set spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict")
    spark.read.parquet("/import/transpaas_data/t_phone_home_work_rela")
      .write
      .mode("append")
      .format("Hive")
      .partitionBy("source", "year", "month", "day")
      .saveAsTable("transpaas_tag.t_phone_home_work_rela")

    spark.sql("set spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict")
    spark.read.parquet("/import/transpaas_data/t_phone_home_work_rela")
      .write
      .mode("append")
      .format("Hive")
      .partitionBy("source", "year", "month", "day")
      .saveAsTable("transpaas_tag.t_phone_home_work_rela")

    spark.sql("set spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict")
    spark.read.parquet("/import/transpaas_data/t_phone_work_distribute")
      .write
      .mode("append")
      .format("Hive")
      .partitionBy("source", "city", "year", "month", "day")
      .saveAsTable("transpaas_tag.t_phone_work_distribute")


    spark.sql("set spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict")
    spark.read.parquet("/import/transpaas_data/t_phone_trip_distribute")
      .write
      .mode("append")
      .format("Hive")
      .partitionBy("source", "start_city", "year", "month", "day")
      .saveAsTable("transpaas_tag.t_phone_trip_distribute")


    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("set spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict")
    spark.read.parquet("/import/transpaas_data/t_transit_people_ic")
      .write
      .mode("append")
      .format("Hive")
      .partitionBy("city", "year", "month", "day")
      .saveAsTable("transpaas_tag.t_transit_people_ic")


    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("set spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict")
    spark.read.parquet("/import/transpaas_data/t_vehicle_gps_bus")
      .write
      .mode("append")
      .format("Hive")
      .partitionBy("city", "year", "month", "day")
      .saveAsTable("transpaas_tag.t_vehicle_gps_bus")
      
    spark.stop()

    println("耗时：" + DateTime.now().minus(startTime.getMillis).getMillis / 1000 + " seconds")
  }

}
```

## 数据同步 RDBMS to Hive 

```scala
object SparkHiveImport {

  def main(args: Array[String]): Unit = {
    val startTime = DateTime.now()

    val app = s"${this.getClass.getSimpleName}".filter(!_.equals('$'))

    Logger.getLogger("org").setLevel(Level.ERROR)


    //TODO:spark初始化
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(app)
      .config("spark.sql.parquet.writeLegacyFormat", true)
      //      .enableHiveSupport()
      .getOrCreate()


    //    val columnName: String = "fdate"
    //    val lowerBound: Long = 20171001
    //    val upperBound: Long = 20171031
    //    val numPartitions: Int = 62

    //    val fromDS = spark.read.jdbc(transpass_std_url, "t_carbon_em_link", columnName, lowerBound, upperBound, numPartitions, transpass_prop).createOrReplaceTempView("tmp")


    //    val predicates =
    //      Array(
    //        "2015-09-16" -> "2015-09-30",
    //        "2015-10-01" -> "2015-10-15",
    //        "2015-10-16" -> "2015-10-31",
    //        "2015-11-01" -> "2015-11-14",
    //        "2015-11-15" -> "2015-11-30",
    //        "2015-12-01" -> "2015-12-15"
    //      ).map {
    //        case (start, end) =>
    //          s"cast(modified_time as date) >= date '$start' " + s"AND cast(modified_time as date) <= date '$end'"
    //      }

    //    val predicates = arr.map(i => {
    //      s"time = $i"
    //    }).toArray

    //
    //    // 取得该表数据
    //    val jdbcDF = sqlContext.read.jdbc(url,tableName,predicates,prop)

    //    import scala.collection.mutable.ArrayBuffer
    //    val arr = ArrayBuffer[Int]()
    //    for (i <- 20110201 until 20110232) {
    //      arr.append(i)
    //    }

    //    val predicates = arr.map(i => {s"SHA1(time)%100 = $i"}).toArray
    //    spark.read.jdbc(transpass_std_url, table, transpass_prop).createOrReplaceTempView("tmp")

    //    val predicates =
    //      Array(
    //        20120201 -> 20120210, 20120211 -> 20120221, 20120222 -> 20120231,
    //        20120301 -> 20120310, 20120311 -> 20120321, 20120322 -> 20120331,
    //        20120401 -> 20120410, 20120411 -> 20120421, 20120422 -> 20120431,
    //        20120501 -> 20120510, 20120511 -> 20120521, 20120522 -> 20120531,
    //        20120601 -> 20120610, 20120611 -> 20120621, 20120622 -> 20120631,
    //        20120701 -> 20120710, 20120711 -> 20120721, 20120722 -> 20120731,
    //        20120801 -> 20120810, 20120811 -> 20120821, 20120822 -> 20120831,
    //        20120901 -> 20120910, 20120911 -> 20120921, 20120922 -> 20120931,
    //        20121001 -> 20121010, 20121011 -> 20121021, 20121022 -> 20121031,
    //        201211601 -> 20121110, 20121111 -> 20121121, 20121122 -> 20121131,
    //        201212601 -> 20121210, 20121211 -> 20121221, 20121222 -> 20121231
    //
    //      ).map {
    //        case (start, end) =>
    //          s"time >= $start " + s" AND time <= $end"
    //      }


    //开启动态分区
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    //hive强制分桶
    spark.sql("set hive.enforce.bucketing=true")
    spark.sqlContext.setConf("spark.sql.hive.convertMetastoreParquet", "false")

    /** ****************/

    import java.util.Properties
    val transpass_db = "tpmodel"
    val table = "T_VPM_VEH_TRIP"
    val transpass_std_url = s"jdbc:oracle:thin:@10.10.180.65:1522/$transpass_db"
    val transpass_prop = new Properties()
    transpass_prop.put("user", "sa")
    transpass_prop.put("password", "Sa123456")
    transpass_prop.put("driver", "oracle.jdbc.driver.OracleDriver")
    transpass_prop.setProperty("fetchsize", "500000")

    var d = 20190702
    import org.joda.time.DateTime
    import org.joda.time.format.DateTimeFormat

    import scala.collection.mutable.ArrayBuffer
    val arr = ArrayBuffer[Int]()

    while (d <= 20190702) {
      arr.append(d)
      d = DateTime.parse(String.valueOf(d), DateTimeFormat.forPattern("yyyyMMdd")).plusDays(1).toString("yyyyMMdd").toInt
    }

    val predicates = arr.map(i => {
      s" fdate = $i"
    }).toArray

    val df = spark.read.jdbc(transpass_std_url, table, predicates, transpass_prop)
    //      println(df.rdd.getNumPartitions)
    df.createOrReplaceTempView("tmp")

    val sql = "select * from tmp"
    spark.sql(sql).write.mode("append").partitionBy("fdate").parquet("/data/T_VPM_VEH_TRIP")


//    20190924
    /** ****************************/

    //    import java.util.Properties
    //    val transpass_db = "bigdata"
    //    val table = "WEATHER_HIS"
    //    val transpass_std_url = s"jdbc:oracle:thin:@10.10.2.40:1521/$transpass_db"
    //    val transpass_prop = new Properties()
    //    transpass_prop.put("user", "SA")
    //    transpass_prop.put("password", "Sa332506")
    //    transpass_prop.put("driver", "oracle.jdbc.driver.OracleDriver")
    //    transpass_prop.setProperty("fetchsize", "500000")
    //
    //    val outputDB = "transpass_origin"
    //


    //    for (y <- Range(2010, 2020)) {
    //      val year = y.toString
    //      val end = (year + "1231").toInt
    //      import org.joda.time.DateTime
    //      import org.joda.time.format.DateTimeFormat
    //      import scala.collection.mutable.ArrayBuffer
    //      val arr = ArrayBuffer[Int]()
    //
    //      var i = (year + "0101").toInt
    //      while (i <= end) {
    //        i = DateTime.parse(String.valueOf(i), DateTimeFormat.forPattern("yyyyMMdd")).plusDays(1).toString("yyyyMMdd").toInt
    //        arr.append(i)
    //      }
    //
    //      val predicates = arr.map(i => {
    //        s" time = $i"
    //      }).toArray
    //
    //      val df = spark.read.jdbc(transpass_std_url, table, predicates, transpass_prop)
    //      //      println(df.rdd.getNumPartitions)
    //      df.createOrReplaceTempView("tmp")
    //
    //      val sql = "select * from tmp"
    //      spark.sql(sql).write.mode("append").partitionBy("time").parquet("/data/origin/weather/his/" + year)
    //    }


    //////////////////////////////////////


    //    for (i <- Range(20160101, 20190919)) {
    //      val sql =
    //        s"""
    //            select * from tmp WHERE time= $i
    //          """.stripMargin
    //
    //      spark.sql(sql).write.mode("append").partitionBy("time").parquet("/data/origin/weather/his")
    //
    //    }

    //.format("Hive")
    //    spark.table(s"$outputDB.$table").show(false)

    spark.stop()

    println("耗时：" + DateTime.now().minus(startTime.getMillis).getMillis / 1000 + " seconds")
  }

}

```


## 数据同步 hive to rdmbs

```scala
object SparkHiveExport {

  def main(args: Array[String]): Unit = {
    val startTime = DateTime.now()

    val app = s"${this.getClass.getSimpleName}".filter(!_.equals('$'))

    Logger.getLogger("org").setLevel(Level.ERROR)


    //TODO:spark初始化
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(app)
      .config("spark.sql.parquet.writeLegacyFormat", true)
      //      .enableHiveSupport()
      .getOrCreate()


    import java.util.Properties
    val transpass_db = "stdbase"
    val transpass_std_url = s"jdbc:postgresql://10.10.201.28:5432/$transpass_db"
    val transpass_url = s"jdbc:postgresql://10.10.201.28:5432/postgres"
    val transpass_prop = new Properties()
    transpass_prop.put("user", "postgres")
    transpass_prop.put("password", "123456")
    transpass_prop.put("driver", "org.postgresql.Driver")

    val sc = spark.sparkContext

    sc.textFile("file:////var/lib/hadoop-hdfs/data/20190916/*.txt").take(10).foreach(println)

    //    val sql =
    //      """
    //            SELECT *
    //            FROM transpass_tag.t_transit_people_ic
    //            WHERE city = 440300
    //             AND YEAR='2017'
    //              AND MONTH='10'
    //              AND DAY ='18'
    //              AND (ftime LIKE '17%'
    //              OR ftime LIKE '18%'
    //              OR ftime LIKE '19%')
    //              AND trans_type in (3,4)
    //          """.stripMargin
    var sql =
    """
            SELECT *
            FROM transpass_tag.t_vehicle_gps_bus
            WHERE city = 440300 and  lng BETWEEN 114.04465 AND 114.06323
   AND lat BETWEEN 22.53088 AND 22.55148
             AND YEAR='2017'
              AND MONTH='10'
              AND DAY ='18'
              AND (ftime LIKE '17%'
              OR ftime LIKE '18%'
              OR ftime LIKE '19%')
          """.stripMargin

    //    val sql3 ="select * from transpass_tag.tmp_t_vehicle_gps_taxi_21081018"

    spark.sql(sql).write.mode("overwrite").jdbc(transpass_std_url, "tmp_vehicle_gps_bus_20171018_late_peak", transpass_prop)

    spark.stop()

    //    spark-shell --master yarn-client --executor-cores 5 --conf spark.sql.adaptive.enabled=true --conf spark.blacklist.enabled=false --num-executors 5  --executor-memory 6g --driver-memory 1g

    println("耗时：" + DateTime.now().minus(startTime.getMillis).getMillis / 1000 + " seconds")
  }

```

## HDFS 数据监控

```scala
object HdfsStatJob {

  def main(args: Array[String]): Unit = {
    val startTime = DateTime.now()

    val app = s"${this.getClass.getSimpleName}".filter(!_.equals('$'))

    Logger.getLogger("org").setLevel(Level.ERROR)

    //TODO:参数验证
    require(args.length >= 1, s"Usage: $app " + "<date>")

    val yesterday = args(0)

    println("参数")
    args.foreach(println)


    //TODO:spark初始化
    //    val spark = SparkSession.builder().master("yarn").appName(app).getOrCreate()

    val spark = SparkSession.builder().master("local[*]").appName(app).getOrCreate()

    import spark.implicits._

    val df = spark.read.jdbc(transpass_std_url, "public.stat_bigdata_hdfs_stat_cfg", transpass_prop).as[HDFS_STAT_CFG].cache()

    //    df.show(false)

    val res = df.mapPartitions(rows => {
      val conf = new Configuration
      conf.set("HADOOP_USER_NAME", "hdfs")
      conf.set("HADOOP_ROOT_LOGGER", "WARN")
      conf.set("fs.defaultFS", "hadoop.user")

      //    conf.addResource("hdfs-site.xml")
      //    conf.addResource("core-site.xml")

      val HDFSUri: String = "hdfs://node2.sutpc.cc:8020"
      val hdfs = FileSystem.get(new URI(HDFSUri), conf, "hdfs")

      rows.map(x => {
        println(x.category + x.name)

//        val yesterDay = DateTime.now().minusDays(1).toString("yyyyMMdd")

        val date = yesterday.substring(0, 4) + "/" + yesterday.substring(4, 6) + "/" + yesterday.substring(6, 8)

//                val date = "2019/08/12"

        val cnt = dirCnt(hdfs, x.path + "/" + x.city_pinyin + "/" + date)

        HDFS_STAT_DAILY(
          UUID.randomUUID().toString.replaceAll("-", ""): String,
          x.category: String,
          x.name: String,
          x.path: String,
          x.city: String,
          x.city_pinyin: String,
          date: String,
          cnt: Long,
          new Timestamp(System.currentTimeMillis()): Timestamp,
          "zhangyongtian": String
        )
      })
    })

    res.show(false)

    res
      .write
      .mode(SaveMode.Append)
      .jdbc(transpass_std_url, "public.stat_bigdata_hdfs_stat_daily", transpass_prop)

    spark.stop()

    println("耗时：" + DateTime.now().minus(startTime.getMillis).getMillis / 1000 + " seconds")
  }


  def dirCnt(hdfs: FileSystem, dir: String): Long = {

    println("统计路径:" + dir)

    val filenamePath = new Path(dir)

    if (!hdfs.exists(filenamePath)) {
      return -1L;
    }

    // 显示实际的输出，例如这里显示 1G
    val contentSummary = hdfs.getContentSummary(filenamePath)

    //会根据集群的配置输出，例如我这里输出3G
    //println("SIZE OF THE HDFS DIRECTORY : " + hdfs.getContentSummary(filenamePath).getSpaceConsumed)

    val dir_size = contentSummary.getLength / 1024 / 1024

    //    println("子目录数：" + contentSummary.getDirectoryCount)
    //    println("子文件数：" + contentSummary.getFileCount)
    //    println("目录空间大小: " + dir_size + "MB")

    dir_size
  }

}
```

## 数据同步 file to hbase

```scala
 val startTime = DateTime.now()
 val app = s"${this.getClass.getSimpleName}".filter(!_.equals('$'))
    Logger.getLogger("org").setLevel(Level.ERROR)
    //TODO:spark初始化
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(app)
      .config("spark.sql.parquet.writeLegacyFormat", true)
      //      .enableHiveSupport()
      .getOrCreate()

 import java.util.Properties
 val sc = spark.sparkContext 
 val conf = HBaseConfiguration.create();
        val tableName = "data1"
        val table = new HTable(conf,tableName)
        conf.set(TableOutputFormat.OUTPUT_TABLE,tableName)

        lazy val job = Job.getInstance(conf)
        job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
        job.setMapOutputValueClass(classOf[KeyValue])
        HFileOutputFormat.configureIncrementalLoad(job,table)

        val rdd = sc.textFile("/data/produce/2015/2015-03-01.log")
          .map(_.split("@"))
          .map{x => (DigestUtils.md5Hex(x(0)+x(1)).substring(0,3)+x(0)+x(1),x(2))}
          .sortBy(x =>x._1)
          .map{x=>{val kv:KeyValue = new KeyValue(Bytes.toBytes(x._1),Bytes.toBytes("v"),Bytes.toBytes("value"),Bytes.toBytes(x._2+"")) (new ImmutableBytesWritable(kv.getKey),kv)}}

        val tmpDir = "/tmp/data1"+System.currentTimeMillis()
        rdd.saveAsNewAPIHadoopFile(tmpDir,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat],job.getConfiguration())
        val bulkLoader = new LoadIncrementalHFiles(conf)
        bulkLoader.doBulkLoad(new Path(tmpDir),table)

        val conf = HBaseConfiguration.create()
        conf.set(TableInputFormat.INPUT_TABLE, "data1")
        val table = new HTable(conf, "data1")
        val put = new Put(Bytes.toBytes("rowKey"))

        put.add("cf", "q", "value")

        for (i <- 1 to 5) {
          var put = new Put(Bytes.toBytes("row" + i))
          put.add(Bytes.toBytes("v"), Bytes.toBytes("value"), Bytes.toBytes("value" + i))
          table.put(put)
        }

        sc.textFile("hdfs://hadoop-ha-cluster/data/2*/2016*/*")
          .map(line => (line.split(",")(0), (line.split(",")(1), line.split(",")(2).toLong, line.split(",")(3).toLong)))
          //      .reduceByKey((tuple1,tuple2)=>(tuple1._1+tuple2._1,tuple1._2+tuple2._2))
          .foreachPartition {
          iterators => {
            var tmpConf = HBaseConfiguration.create()
            tmpConf.set("hbase.zookeeper.quorum", "hadoop31,hadoop32,hadoop33,hadoop34,hadoop35")
            tmpConf.set("hbase.zookeeper.property.clientPort", "2181")
            var table = new HTable(tmpConf, "hbase_test")
    //        table.setWriteBufferSize(5 * 1024 * 1024)
            var putList = new java.util.ArrayList[Put]
            iterators.foreach { tupple => {
              var fixednumber = ("0" * (25 - tupple._1.length) + tupple._1).reverse
              var rowkey = fixednumber.getBytes()
              var p = new Put(rowkey)
              p.add("comumnfamily".getBytes, "column1".getBytes, tupple._2._1.toString.getBytes)
              p.add("comumnfamily".getBytes, "column2".getBytes, tupple._2._2.toString.getBytes)

              putList.add(p)
              if (putList.size() > 0 && putList.size() % 1000 == 0) {
                table.put(putList)
                putList.clear()
              }
            }
            }
            table.put(putList)
          }
        }
```
