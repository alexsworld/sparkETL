
@[TOC](目录)

# 架构
common
conf
	JobConfigReader
	JobLock
	JobLogWriter
parser
	HdfsInputReader
register
	TaskRegister
schedule
	JobRunner
storage
	TaskOutput
task 
	BaseTask
	CommonTasks
	HisTasks
	Tasks
EtlApp

# 主类

```scala
object EtlApp {

  def main(args: Array[String]): Unit = {

    val startTime = DateTime.now()

    val app = s"${this.getClass.getSimpleName}".filter(!_.equals('$'))

    Logger.getLogger("org").setLevel(Level.ERROR)

    //TODO:spark初始化
        val spark = SparkSession
          .builder()
          .master("yarn")
          .appName(app)
          .config("spark.sql.parquet.writeLegacyFormat", true)
          .enableHiveSupport()
          .getOrCreate()

//    val spark = SparkSession.builder().master("local[*]").appName(app).getOrCreate()

    TaskParser.parse(spark, JobConfigReader.getJobConfig(spark))

    spark.stop()

    println("耗时：" + DateTime.now().minus(startTime.getMillis).getMillis / 1000 + " seconds")
  }


}

```



# ELT 配置表

```scala
case class EtlJobConfig(
                         id: Int,
                         task_name: String,
                         priority: Int,
                         input: String,
                         input_type: String,
                         input_db: String,
                         output: String,
                         output_type: String,
                         output_db: String,
                         frequency: String,
                         task_type: Int,
                         is_enable: Int,
                         encode: String = "UTF-8",
                         parsed_date: String
 )
           
case class EtlJobLog(task_id: Int, task_name: String, start_time: Timestamp, stop_time: Timestamp, input: String, output: String, remark: String)
            
```

# conf 

## DBUrls

```scala

object DBUrls {
  val etl_conf_url = "jdbc:mysql://xxxx:3306/etl?autoReconnect=true&useUnicode=true&characterEncoding=utf-8&allowMultiQueries=true"
  val etl_conf_prop = new Properties()
  etl_conf_prop.put("user", "root")
  etl_conf_prop.put("password", "root")
  etl_conf_prop.put("driver", "com.mysql.jdbc.Driver")
}
```

## JobConfigReader 

```scala
object JobConfigReader {

  def getJobConfig(spark: SparkSession, table: String = "etl_job_config"): Array[EtlJobConfig] = {

    println("读取任务配置")

    import spark.implicits._
    val df = spark.read.jdbc(etl_conf_url, table, etl_conf_prop).where($"is_enable" === 1).orderBy("priority", "id").as[EtlJobConfig].cache()
    df.show(false)
    df.collect()
  }


}

```

## JobLock 

```scala
object JobLock {

  var isDropPartition = true
  var isDropPartitionMonth = true
  var isDropPartitionYear = true
}


```


## JobLogWriter 
```scala
object JobLogWriter {

  def log(jobLog: EtlJobLog, spark: SparkSession) = {

    println("写任务日志")

    import spark.implicits._
    spark.createDataset(Seq(jobLog)).write.mode("append").jdbc(etl_conf_url, "etl_job_log", etl_conf_prop)
  }

}
```

# Parser
## TaskParser 

```scala
object TaskParser {

  def parse(spark: SparkSession, tasks: Array[EtlJobConfig]): Unit = {

    tasks.foreach(t => {
      println("任务【(" + t.id + ")" + t.task_name + ":" + t.input + "->" + t.output + "表】正在注册执行...")
      JobLogWriter.log(EtlJobLog(t.id, t.task_name, new Timestamp(System.currentTimeMillis()), null, t.input, t.output, null), spark)

      t.task_type match {

        //临时任务
        case 0 => TaskRegister.register(spark, TempTaskParser.parse(t))

        //周期性任务
        case 1 => TaskRegister.register(spark, CycleTaskParser.parse(t))

        case 2 => println("任务类型未知"); System.exit(1)
      }

    })
  }
}
```

## TempTaskParser 

```scala
object TempTaskParser {

  def parse(task: EtlJobConfig): Array[EtlJobConfig] = {

    if (task.input_type.equals("hive") && task.output_type.equals("db")) {
      require(Array("append", "overwrite").contains(task.frequency), "hive2db 写入模式非法！")
      return Array[EtlJobConfig](task)
    }

    if (task.input_type.equals("db") && task.output_type.equals("db")) {
      require(Array("append", "overwrite").contains(task.frequency), "db2db 写入模式非法！")
      return Array[EtlJobConfig](task)
    }



    require(task.frequency.split(",").length == 2, "开始和结束日期格式不正确")
    val range = task.frequency.split(",")
    val (startDate, stopDate) = (range(0).trim, range(1).trim)
    val startTime = DateTime.parse(startDate, DateTimeFormat.forPattern("yyyy/MM/dd"))
    val stopTime = DateTime.parse(stopDate, DateTimeFormat.forPattern("yyyy/MM/dd"))
    require(startTime.getMillis <= stopTime.getMillis, "开始日期必须小于结束日期")

    TimeRangeParser.parse(task, startTime, stopTime)
  }
}
```


```scala
object TimeRangeParser {

  def parse(task: EtlJobConfig, startTime: DateTime, stopTime: DateTime): Array[EtlJobConfig] = {

    val arr = new ArrayBuffer[EtlJobConfig]()
    var time = startTime
    while (time.getMillis <= stopTime.getMillis) {
      arr.append(task.copy(parsed_date = time.toString("yyyy/MM/dd")))
      time = TimeScheduler.next("day", 1, time)
    }
    arr.toArray

  }

}

```

## CycleTaskParser 

```scala
object CycleTaskParser {

  def parse(task: EtlJobConfig): Array[EtlJobConfig] = {

    task.frequency match {
      case "daily" => {
        Array(task.copy(parsed_date = DateTime.now().minusDays(1).toString("yyyy/MM/dd")))
      }
      //非自然周
      case "weekly" => {
        TimeRangeParser.parse(task, DateTime.now().minusWeeks(1), DateTime.now().minusWeeks(1).plusDays(6))
      }
      //非自然月
      case "monthly" => {
        TimeRangeParser.parse(task, DateTime.now().minusMonths(1), DateTime.now())
      }
      case _ => println("无效的日期格式"); System.exit(1); Array(task)
    }
  }
}

```


## HdfsInputParser 

```scala
object HdfsInputParser {

  def parse(spark: SparkSession, task: EtlJobConfig): EtlJobConfig = {
    task.copy(input = task.input + "/" + task.parsed_date)
  }
}
```

#  register
## TaskRegister

```scala
object TaskRegister {

  def register(spark: SparkSession, tasks: Array[EtlJobConfig]) = {

    tasks.foreach(t => {

      if (t.input_type.equals("hdfs") && t.output_type.equals("hive")) {
        val task = HdfsInputParser.parse(spark, t)
        task.task_name match {
          case "s_vehicle_gps_others_real" => new RealTimeGpsOthersTask(spark, task).execute()
          case "s_vehicle_gps_taxi_real" => new RealTimeGpsTaxiExecutor(spark, task).execute()
          case "s_vehicle_gps_bus_real" => new RealTimeGpsBusExecutor(spark, task).execute()
          case "s_vehicle_gps_order_real" => new RealTimeGpsOrderExecutor(spark, task).execute()

          case "s_vehicle_gps_taxi_his" => new HisGpsTaxiExecutor(spark, task).execute()
          case _ => println("找不到目标表")
        }
      }

      if (t.input_type.equals("db") && t.output_type.equals("hive")) {
        t.task_name match {
          case "db2hive_partition" => new DB2HivePartitionExecutor(spark, t).execute()
          case _ => println("找不到目标表")
        }
      }

      if (t.input_type.equals("hive") && t.output_type.equals("db")) {
        t.task_name match {
          case "hive2db_simple" => new Hive2DBSimpleExecutor(spark, t).execute()
          case _ => println("找不到目标表")
        }
      }

      if (t.input_type.equals("db") && t.output_type.equals("db")) {
        t.task_name match {
          case "db2db_simple" => new DB2DBSimpleExecutor(spark, t).execute()
          case _ => println("找不到目标表")
        }
      }


      //      println("任务输入类型未知");
      //      System.exit(1);

    })


  }

}
```

# schedule

## JobRunner


# storage

```scala
object TaskOutput {


  def hive(spark: SparkSession, task: EtlJobConfig, partitions: Array[String], res: DataFrame) = {
    //开启动态分区
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("set hive.exec.max.dynamic.partitions=90000")

    //hive强制分桶
    spark.sql("set hive.enforce.bucketing=true")
    spark.sqlContext.setConf("spark.sql.hive.convertMetastoreParquet", "false")

    task.output.split(",").foreach(t => {

      println("建表")
      res.where("1=0")
        //        .coalesce(paraNum)
        .write
        .mode(SaveMode.Append)
        .format("Hive")
        .partitionBy(partitions: _*)
        .saveAsTable(t)
      

      println("输出到hive的表【" + t + "】")
      if (t.startsWith("transpaas_std_dev")) {
        println("删除七天前的数据")
        val lastWeek = DateTime.now().minusDays(7)
        val year = lastWeek.getYear
        val month = lastWeek.getMonthOfYear
        val day = lastWeek.getDayOfMonth
        val sql = s"alter table $t drop  if exists  partition (year=$year,month=$month,day=$day)"
        println(sql)
        spark.sql(sql)
      }


      println("删除已存在的数据")
      val dateTime = DateTime.parse(task.parsed_date, DateTimeFormat.forPattern("yyyy/MM/dd"))
      val sql = s"alter table $t drop  if exists  partition (year=${dateTime.getYear},month=${dateTime.getMonthOfYear},day=${dateTime.getDayOfMonth})"
      if (JobLock.isDropPartition) {
        println(sql)
        spark.sql(sql)
      }

      println("输出hive")
      res
        //        .coalesce(paraNum)
        .write
        .mode(SaveMode.Append)
        .format("Hive")
        .partitionBy(partitions: _*)
        .saveAsTable(t)

      println("当前时间：" + DateTime.now().toString)

    })

  }


  def hbase[T](zkUrl: String, outputTable: String, res: Dataset[T]) = {

    println("输出到HBase的表" + outputTable)

    res.distinct.write
      .format("org.apache.phoenix.spark")
      .mode("overwrite")
      .option("phoenix.schema.isNamespaceMappingEnabled", "true")
      .option("table", outputTable)
      .option("zkUrl", zkUrl)
      .save()

    println("当前时间：" + DateTime.now().toString)
  }


  def hive[T](spark: SparkSession, task: EtlJobConfig, partitions: Array[String], res: Dataset[T], paraNum: Int = 16, isPersist: Boolean = true, db: String = "transpass_std", isCopy2Tag: Boolean = false, dupliCols: Seq[String]) = {


    //开启动态分区
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("set hive.exec.max.dynamic.partitions=90000")

    //hive强制分桶
    spark.sql("set hive.enforce.bucketing=true")
    spark.sqlContext.setConf("spark.sql.hive.convertMetastoreParquet", "false")

    task.output.split(",").foreach(t => {
      println("输出到hive的表【" + t + "】")
      if (t.startsWith("transpaas_std_dev")) {
        println("删除七天前的数据")
        val lastWeek = DateTime.now().minusDays(7)
        val year = lastWeek.getYear
        val month = lastWeek.getMonthOfYear
        val day = lastWeek.getDayOfMonth
        val sql = s"alter table $t drop  if exists  partition (year=$year,month=$month,day=$day)"
        println(sql)
        spark.sql(sql)
      }

      println("删除已存在的数据")
      task.parsed_date.length match {
        case 10 => {
          val dateTime = DateTime.parse(task.parsed_date, DateTimeFormat.forPattern("yyyy/MM/dd"))
          val sql = s"alter table $t drop  if exists  partition (year=${
            dateTime.getYear
          },month=${
            dateTime.getMonthOfYear
          },day=${
            dateTime.getDayOfMonth
          })"
          if (JobLock.isDropPartition) {
            println(sql)
            spark.sql(sql)
          }
        }
        case 7 => {
          val dateTime = DateTime.parse(task.parsed_date, DateTimeFormat.forPattern("yyyy/MM"))
          val sql = s"alter table $t drop  if exists  partition (year=${
            dateTime.getYear
          },month=${
            dateTime.getMonthOfYear
          })"
          if (JobLock.isDropPartitionMonth) {
            println(sql)
            spark.sql(sql)
            JobLock.isDropPartitionMonth = false
          }
        }
        case 4 => {
          val dateTime = DateTime.parse(task.parsed_date, DateTimeFormat.forPattern("yyyy"))
          val sql = s"alter table $t drop  if exists  partition (year=${
            dateTime.getYear
          }"
          if (JobLock.isDropPartitionYear) {
            println(sql)
            spark.sql(sql)
            JobLock.isDropPartitionYear = false
          }
        }
        case _ => println("日期格式不正确");
          System.exit(1)
      }

      println("输出hive")
      res
        //        .coalesce(paraNum)
        .write
        .mode(SaveMode.Append)
        .format("Hive")
        .partitionBy(partitions: _*)
        .saveAsTable(t)
    })

    if (isPersist) res.unpersist()

    println("当前时间：" + DateTime.now().toString)
  }


  def parquet[T](spark: SparkSession, outputTable: String, partitions: Array[String], res: Dataset[T], paraNum: Int = 16) = {

    println("输出到parquet" + outputTable)

    res.distinct.coalesce(paraNum)
      .write
      .mode(SaveMode.Append)
      .partitionBy(partitions: _*)
      .parquet("/parquet/" + outputTable)

    res.unpersist()

    println("当前时间：" + DateTime.now().toString)
  }

}
```

# Task 

```scala
trait BaseTask[T] extends Serializable {

  def hdfs2RDD(inputPath: String, spark: SparkSession, task: EtlJobConfig) = {
    println("读取【" + inputPath + "】数据")

    val sc = spark.sparkContext
    var charset = task.encode

    if (task.encode.equals("#")) {
      println("检测编码格式")
      val samples = spark.read.textFile(inputPath).filter(!_.isEmpty).take(1)
      charset = CharsetUtils.getEncode(samples(0))
    }

    println("编码格式" + charset)

    sc.hadoopFile(inputPath, classOf[TextInputFormat],
      classOf[LongWritable], classOf[Text])
      .map(pair => new String(pair._2.getBytes, 0, pair._2.getLength, charset))
  }

  def filter(x: String): Boolean

  def execute()

  def smartLoop(inputPath: String, spark: SparkSession) = {
    val path = new Path(inputPath)
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)

    var flag = false


    if (hdfs.exists(path)) {

      val stats = hdfs.listStatus(path)
      //      val arr = hdfs.listFiles(path, false)

      //      import collection.JavaConversions._
      //      import collection.convert.wrapAsScala._ //只是完成 Java  到 Scala 集合的隐式转换

      for (i <- 0 to stats.length - 1) {
        if (stats(i).isFile) {
          flag = false
        } else if (stats(i).isDirectory) {
          flag = true
        } else if (stats(i).isSymlink) {
          flag = false
        }
      }


      //全是目录
      if (flag) {
        for (i <- 0 to stats.length - 1) {
          val e = stats(i)
          println("循环读取子目录：" + e.getPath.toString)
          process(e.getPath.toString)
        }
      }
      if (!flag) {
        println("读取单个目录")
        process(inputPath)
      }

    } else {
      println("输入路径【" + path + "】不存在，已跳过")
    }


  }

  def process(dir: String)

  def cache(res: Dataset[T]): Unit = {
    //    res.persist(StorageLevel.MEMORY_AND_DISK_SER)
    //    println("输入数据量【" + res.count() + " 】")
    res.show(false)
  }

}
```

## DB2HivePartitionExecutor

```scala
class DB2HivePartitionExecutor(spark: SparkSession, task: EtlJobConfig) {

  def execute() = {

    val json = JSON.parseObject(task.input)
    val r = json.containsKey("driver") && json.containsKey("url") && json.containsKey("user") && json.containsKey("password") && json.containsKey("table") && json.containsKey("dateCol") && json.containsKey("where")
    require(r, "数据库信息缺失")

    //    "driver":"org.postgresql.Driver",
    //    "url":"jdbc:postgresql://10.10.201.28:5432/stdbase",
    //    "user":"postgres",
    //    "password":"123456",
    //    "table":"t_common_base_roadsect",
    //    "splitBy":"",
    //    "where":"",
    //    "sql":""

    val driver = json.getString("driver").trim
    val url = json.getString("url").trim
    val user = json.getString("user").trim
    val password = json.getString("password").trim
    val table = json.getString("table").trim

    //    val dateCol = if (json.containsKey("dateCol")) json.getString("dateCol").trim else null
    //    val datePattern = if (json.containsKey("datePattern")) json.getString("datePattern").trim else null

    //    val sql = if(json.containsKey("sql")) json.getString("sql").trim else null

    //fdate = $i to_char(sysdate,'yyyy-mm-dd hh24:mi:ss')

    val prop = new Properties()
    prop.put("user", user)
    prop.put("password", password)
    prop.put("driver", driver)
    prop.setProperty("fetchsize", "5000")

    val whereCondition = json.getString("where") + s"='${task.parsed_date}" + "'"
    println(whereCondition)
    val predicates = Array(whereCondition)

    spark.read.jdbc(url, table, predicates, prop).createOrReplaceTempView("tmp_table")

    //    val dateCol = json.getString("dateCol")

    val dateTime = DateTime.parse(task.parsed_date, DateTimeFormat.forPattern("yyyy/MM/dd"))
    val year = dateTime.getYear
    val month = dateTime.getMonthOfYear
    val day = dateTime.getDayOfMonth

    val res = spark.sql(s"select *,$year year,$month month,$day day from tmp_table")

    TaskOutput.hive(spark, task, Array("year", "month", "day"), res)
    JobLogWriter.log(EtlJobLog(task.id, task.task_name, null, new Timestamp(System.currentTimeMillis()), task.input, task.output, null), spark)

  }

}
```


## Hive2DBSimpleExecutor

```scala
class Hive2DBSimpleExecutor(spark: SparkSession, task: EtlJobConfig) {

  def execute() = {

    val json = JSON.parseObject(task.output)
    val r = json.containsKey("driver") && json.containsKey("url") && json.containsKey("user") && json.containsKey("password") && json.containsKey("table")
    require(r, "数据库信息缺失")

    val driver = json.getString("driver").trim
    val url = json.getString("url").trim
    val user = json.getString("user").trim
    val password = json.getString("password").trim
    val table = json.getString("table").trim

    val prop = new Properties()
    prop.put("user", user)
    prop.put("password", password)
    prop.put("driver", driver)
    prop.setProperty("fetchsize", "5000")

    spark.sql(task.input).distinct.write.mode(task.frequency).jdbc(url, table, prop)
    JobLogWriter.log(EtlJobLog(task.id, task.task_name, null, new Timestamp(System.currentTimeMillis()), task.input, task.output, null), spark)

  }

}
```

## DB2DBSimpleExecutor

```scala
class DB2DBSimpleExecutor(spark: SparkSession, task: EtlJobConfig) {

  def execute() = {

    var json = JSON.parseObject(task.input)
    var condition = json.containsKey("driver") && json.containsKey("url") && json.containsKey("user") && json.containsKey("password") && json.containsKey("table")
    require(condition, "数据库信息缺失")

    var driver = json.getString("driver").trim
    var url = json.getString("url").trim
    var user = json.getString("user").trim
    var password = json.getString("password").trim
    var table = json.getString("table").trim

    var prop = new Properties()
    prop.put("user", user)
    prop.put("password", password)
    prop.put("driver", driver)
    prop.setProperty("fetchsize", "5000")

    val df = spark.read.jdbc(url, table, prop)

    json = JSON.parseObject(task.output)
    condition = json.containsKey("driver") && json.containsKey("url") && json.containsKey("user") && json.containsKey("password") && json.containsKey("table")
    require(condition, "数据库信息缺失")

    driver = json.getString("driver").trim
    url = json.getString("url").trim
    user = json.getString("user").trim
    password = json.getString("password").trim
    table = json.getString("table").trim

    prop.put("user", user)
    prop.put("password", password)
    prop.put("driver", driver)
    prop.setProperty("fetchsize", "5000")

    //str.trim().split('\u0000')[0];
    //invalid byte sequence for encoding "UTF8": 0x00
    df.write.mode(task.frequency).jdbc(url, table, prop)

    JobLogWriter.log(EtlJobLog(task.id, task.task_name, null, new Timestamp(System.currentTimeMillis()), task.input, task.output, null), spark)

  }

}
```

# 自定义Executor

## HisGpsTaxiExecutor

```scala
class HisGpsTaxiExecutor(spark: SparkSession, task: EtlJobConfig) extends BaseTask[STD_GPS_TAXI] {

  def filter(x: String): Boolean = {
    val arr = x.split(",")
    val c1 = !arr(3).isEmpty

    val time_tmp = arr(0) + StdUtils.fillZero(6, arr(1))
    //loc_time	定位日期	整型	10	否		unix10位时间戳
    val dataTime = DateTime.parse(time_tmp, DateTimeFormat.forPattern("yyyyMMddHHmmss"))
    val c2 = dataTime.getMillis <= DateTime.now().getMillis

    c1 && c2

  }

  def map(x: String) = {
    val arr = x.split(",")

    //20171001,13,,粤BM7485,114.023085,22.755496,0,119,1,1
    val time_tmp = arr(0) + StdUtils.fillZero(6, arr(1))

    //loc_time	定位日期	整型	10	否		unix10位时间戳
    val dataTime = DateTime.parse(time_tmp, DateTimeFormat.forPattern("yyyyMMddHHmmss"))
    val loc_time = dataTime.getMillis / 1000

    //vehicle_id	车辆唯一标识（车牌）	字符串		否
    val vehicle_id = arr(3)

    //lng	经度，小数点后取6位	浮点	10,6	否		WGS坐标系
    val lng = BigDecimal(arr(4)).setScale(6)

    //lat	纬度，小数点后取6位	浮点	10,6	否		WGS坐标系
    val lat = BigDecimal(arr(5)).setScale(6)

    //speed	瞬时速度（km/h）	浮点	4,1	否
    val speed = BigDecimal(arr(6)).setScale(1)

    //angle	方向角，与正北方向的顺时针夹角（0-359）	整型	3 	否
    val angle = arr(7).toInt

    //company_code	所属公司代码	字符串		否		公司代码编码
    val company_code = "-99"

    //busline_name	公交线路名称	字符串		否
    val busline_name = arr(2)


    // busline_dir	方向 	整型		是		0为上行，1为下行，2偏离路线，3为上行场区，4为下行场区
    val busline_dir = null

    //bus_station_order	站序	整型		是
    val bus_station_order = null

    //city	城市名称	字符串		否	1	参照城市编码对照表
    val city_name = "shenzhen"

    //深圳
    val city = 440300


    val is_validate = arr(9).toInt

    val bus_position = null

    val alarm_status: String = null
    val vehicle_status: String = null

    val year = arr(0).substring(0, 4).toInt
    val month = arr(0).substring(4, 6).toInt
    val day = arr(0).substring(6, 8).toInt

    //operation_status	运营状态，出租车预留字段	整型	1 	否		1重载，0空载
    val operation_status = arr(8).toInt

    //altitude	海拔高度，单位m	整型		否
    val altitude = -99

    //vehicle_color	车辆颜色	整型	1	是		1-蓝；2-黄；3-黑；4-白；5-其他
    val vehicle_color = null

    //total_mileage	车辆总里程数，单位千米	整型	10	是
    val total_mileage = null

    STD_GPS_TAXI(
      loc_time: Long,
      vehicle_id: String,
      lng: BigDecimal,
      lat: BigDecimal,
      speed: BigDecimal,
      angle: Int,
      is_validate: Int,
      operation_status: Int,
      company_code: String,
      altitude: Int,
      vehicle_color: Integer,
      total_mileage: Integer,
      alarm_status: String,
      vehicle_status: String,
      city: Int,
      year: Int,
      month: Int,
      day: Int
    )
  }

  def execute() = {
    smartLoop(task.input: String, spark: SparkSession)
  }


  def process(dir: String) = {
    val rdd = hdfs2RDD(dir: String, spark: SparkSession, task: EtlJobConfig)
    import spark.implicits._
    val res = spark.createDataset(rdd).as[String]
      .filter(x => filter(x))
      .map(x => map(x))
      .filter(x =>
        x.vehicle_id.nonEmpty && x.lng != null && x.lat != null && StdUtils.isCnLngLat(x.lng.toDouble, x.lat.toDouble) && x.speed < 900
      ).filter(x => {

      lazy val dataTime = new DateTime(x.loc_time * 1000)
      lazy val today = DateTime.parse(task.input.substring(task.input.lastIndexOf("/") - 7), DateTimeFormat.forPattern("yyyy/MM/dd")).plusDays(1)

      dataTime.getMillis >= today.minusDays(1).getMillis && dataTime.getMillis < today.getMillis
    })

    //    cache(res)

    TaskOutput.hive[STD_GPS_TAXI](spark, task, Array("city", "year", "month", "day"), res, isPersist = false, dupliCols = Seq("vehicle_id", "loc_time"), isCopy2Tag = true)
    JobLogWriter.log(EtlJobLog(task.id, task.task_name, null, new Timestamp(System.currentTimeMillis()), task.input, task.output, null), spark)


  }
}

```


# 其他

```scala
class RealTimeGpsOthersTask(spark: SparkSession, task: EtlJobConfig) extends BaseTask[STD_GPS_OTHER] {

  def filter(x: String): Boolean = {
    val arr = x.split(",")
    val c1 = StdUtils.allNotEmpty(arr.slice(0, 3))

    lazy val time_tmp = arr(0) + StdUtils.fillZero(6, arr(1))
    lazy val dataTime = DateTime.parse(time_tmp, DateTimeFormat.forPattern("yyyyMMddHHmmss"))
    lazy val c2 = dataTime.getMillis <= DateTime.now().getMillis

    val c3 = arr(3).length > 2

    lazy val today = DateTime.parse(task.input.substring(task.input.lastIndexOf("/") - 7), DateTimeFormat.forPattern("yyyy/MM/dd")).plusDays(1)

    val c4 = dataTime.getMillis >= today.minusDays(1).getMillis && dataTime.getMillis < today.getMillis

    c1 && !x.contains("grid_id") && c2 && c3 && c4

  }

  def map(x: String) = {
    val arr = x.split(",")

    //20170930,235953,H,粤BT2046,113.902152,34.702252,0.0,1,1,1,2,0,837601,0,2,0
    //20180930,235857,H,粤BW7325,114.242953,22.598443,0.0,87,1,1,2,0,93526,0,3,134217728,4


    //    20171001,23,H,粤BX3340,113.885809,22.503325,0.0,0,1,1,2,0,9390700,13,786434,0
    //定位日期,定位时刻,所属公司代码,车牌号码,经度，纬度，速度，方向角，运营状态，数据可用性，车辆颜色，行车速度 总里程数 海拔高度 车辆状态 报警状态 车辆类型

    //    loc_time	定位日期	整型	10	否		unix10位时间戳
    //    vehicle_id	车辆唯一标识（车牌）	字符串	20	否
    //      lng	经度，小数点后取6位	浮点	10,6	否		WGS坐标系
    //    lat	纬度，小数点后取6位	浮点	10,6	否		WGS坐标系
    //    speed	瞬时速度（km/h）	浮点	4,1	否
    //      angle	方向角，与正北方向的顺时针夹角（0-359）	整型	3 	否

    //    is_validate	数据是否有效，	整型	1	否		0表示异常，1表示有效
    val is_validate = arr(9).toInt

    //      alarm_status	报警状态	字符串		是		存储原始数据，保留以后使用
    val alarm_status = arr(15)

    //    vehicle_status	车辆状态	字符串		是		存储原始数据，保留以后使用
    val vehicle_status = arr(14)

    val date: String = arr(0)
    val time_tmp = date + StdUtils.fillZero(6, arr(1))

    //loc_time	定位日期	整型	10	否		unix10位时间戳
    val dataTime = DateTime.parse(time_tmp, DateTimeFormat.forPattern("yyyyMMddHHmmss"))
    val loc_time = dataTime.getMillis / 1000

    //fdate	日期	整型		否		20190708
    val fdate = date.toInt

    //ftime	时间	字符串		否		190802
    val ftime = dataTime.toString("HHmmss")

    //vehicle_id	车辆唯一标识（车牌）	字符串		否
    val vehicle_id = arr(3)

    //lng	经度，小数点后取6位	浮点	10,6	否		WGS坐标系
    val lng = BigDecimal(arr(4)).setScale(6, RoundingMode.HALF_UP)

    //lat	纬度，小数点后取6位	浮点	10,6	否		WGS坐标系
    val lat = BigDecimal(arr(5)).setScale(6, RoundingMode.HALF_UP)

    //speed	瞬时速度（km/h）	浮点	4,1	否
    val speed = BigDecimal(arr(5)).setScale(1, RoundingMode.HALF_UP)

    //angle	方向角，与正北方向的顺时针夹角（0-359）	整型	3 	否
    val angle = arr(7).toInt

    //operation_status	运营状态，出租车预留字段	整型	1 	否		1重载，0空载
    val operation_status = arr(8).toInt

    //company_code	所属公司代码	字符串		否		公司代码编码
    val company_code = arr(2)

    //altitude	海拔高度，单位m	整型		否
    val altitude = arr(13).toInt

    //vehicle_color	车辆颜色	整型	1	是		1-蓝；2-黄；3-黑；4-白；5-其他
    val vehicle_color = arr(10).toInt

    //total_mileage	车辆总里程数，单位千米	整型	10	是
    val total_mileage = arr(12).toLong

    //city	城市名称	字符串		否	1	参照城市编码对照表
    // 深圳
    val city = "440300".toInt

    //    0-导航小汽车；1-班车客运；2-出租车，4-货运车；8-驾培车；16-包车客运；32-公交车；64-危险品车；128-其他车辆；256-泥头车；512-网约车
    val vehicle_type = arr(16).toInt

    val year: String = arr(0).substring(0, 4)
    val month: String = arr(0).substring(4, 6)
    val day: String = arr(0).substring(6, 8)

    val license_prefix = vehicle_id.substring(0, 2)

    STD_GPS_OTHER(
      loc_time: Long,
      vehicle_id: String,
      lng: BigDecimal,
      lat: BigDecimal,
      speed: BigDecimal,
      angle: Int,
      is_validate: Int,
      vehicle_color: Int,
      total_mileage: Long,
      altitude: Int,
      company_code: String,
      alarm_status: String,
      vehicle_status: String,
      vehicle_type: Int,
      city: Int,
      year: String,
      month: String,
      day: String,
      license_prefix: String
    )

  }

  def execute() = {

    println("读取【" + task.input + "】数据")

    smartLoop(task.input: String, spark: SparkSession)

  }

  def process(dir: String) = {

    val rdd = hdfs2RDD(dir: String, spark: SparkSession, task: EtlJobConfig)
    import spark.implicits._
    val res = spark.createDataset(rdd).as[String]
      .filter(x => filter(x))
      .map(x => map(x)).filter(_.speed < 900)

    //    cache(res)

    TaskOutput.hive[STD_GPS_OTHER](spark, task, Array("vehicle_type", "city", "year", "month", "day"), res, isPersist = false, dupliCols = Seq("vehicle_id", "loc_time"), isCopy2Tag = true)
    JobLock.isDropPartition = false
    JobLogWriter.log(EtlJobLog(task.id, task.task_name, null, new Timestamp(System.currentTimeMillis()), task.input, task.output, null), spark)


  }

}

```

```scala
class RealTimeGpsTaxiExecutor(spark: SparkSession, task: EtlJobConfig) extends BaseTask[STD_GPS_TAXI] {

  def filter(x: String): Boolean = {

    var isOk = true

    Try(JSON.parseArray(x)) match {
      case Success(i) => isOk = true
      case Failure(s) => isOk = false
    }

    lazy val jsonArr = JSON.parseArray(x)
    lazy val jsonObj = jsonArr.getJSONObject(0)

    lazy val time_tmp = jsonObj.getString("locationTime").trim
    //loc_time	定位日期	整型	10	否		unix10位时间戳
    lazy val dataTime = DateTime.parse(time_tmp, DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"))

    lazy val today = DateTime.parse(task.input.substring(task.input.lastIndexOf("/") - 7), DateTimeFormat.forPattern("yyyy/MM/dd")).plusDays(1)

    isOk && dataTime.getMillis >= today.minusDays(1).getMillis && dataTime.getMillis < today.getMillis


  }

  def map(x: String) = {

    val jsonArr = JSON.parseArray(x)
    val jsonObj = jsonArr.getJSONObject(0)


    //      [{"acc":false,"aiLock":false,"altitude":0.0,"carLock":false,"carState":false,"circuitry":false,"color":1,"direction":39,"driveSpeed":0.0,"emptyToHeavy":false,"encrypted":false,"heavyToEmpty":false,"id":"粤BDK0187","latDirection":false,"latitude":22.574913,"lngDirection":false,"location":false,"locationTime":"2019-09-24 23:59:22","longitude":114.06607,"obligate":false,"oil":false,"otherState":false,"price":false,"satelliteSpeed":0.0,"state":false,"totalMileage":1161890.0,"wieldNumberTime":false}]


    val tmpTime = jsonObj.getString("locationTime").trim
    val dataTime = DateTime.parse(tmpTime, DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"))
    val loc_time = dataTime.getMillis / 1000

    val vehicle_id: String = jsonObj.getString("id")
    val lng: BigDecimal = BigDecimal(jsonObj.getString("longitude")).setScale(6)
    val lat: BigDecimal = BigDecimal(jsonObj.getString("latitude")).setScale(6)
    val speed: BigDecimal = BigDecimal(jsonObj.getString("driveSpeed")).setScale(1)
    val angle: Int = jsonObj.getIntValue("direction")
    val is_validate: Int = 1
    val operation_status: Int = if (jsonObj.getBooleanValue("carState")) 1 else 0
    val company_code: String = "-99"
    val altitude: Int = jsonObj.getDouble("altitude").toInt
    val vehicle_color: Integer = jsonObj.getInteger("color")
    val total_mileage: Integer = (jsonObj.getDouble("totalMileage").toInt) / 1000

    val alarm_status: String = null
    val vehicle_status: String = null

    //city	城市名称	字符串		否	1	参照城市编码对照表
    val city = 440300

    val tmpTime2 = dataTime.toString("yyyyMMdd")
    val year = tmpTime2.substring(0, 4).toInt
    val month = tmpTime2.substring(4, 6).toInt
    val day = tmpTime2.substring(6, 8).toInt


    STD_GPS_TAXI(
      loc_time: Long,
      vehicle_id: String,
      lng: BigDecimal,
      lat: BigDecimal,
      speed: BigDecimal,
      angle: Int,
      is_validate: Int,
      operation_status: Int,
      company_code: String,
      altitude: Int,
      vehicle_color: Integer,
      total_mileage: Integer,
      alarm_status: String,
      vehicle_status: String,
      city: Int,
      year: Int,
      month: Int,
      day: Int
    )
  }

  def execute() = {

    smartLoop(task.input: String, spark: SparkSession)
  }

  def process(dir: String) = {
    val rdd = hdfs2RDD(dir: String, spark: SparkSession, task: EtlJobConfig)
    import spark.implicits._
    val res = spark.createDataset(rdd).as[String]
      .filter(x => filter(x))
      .map(x => map(x))
      .filter(x =>
        x.vehicle_id.nonEmpty && x.lng != null && x.lat != null && StdUtils.isCnLngLat(x.lng.toDouble, x.lat.toDouble) && x.speed < 900
      )

    //    cache(res)

    TaskOutput.hive[STD_GPS_TAXI](spark, task, Array("city", "year", "month", "day"), res, isPersist = false, dupliCols = Seq("vehicle_id", "loc_time"), isCopy2Tag = true)

    JobLogWriter.log(EtlJobLog(task.id, task.task_name, null, new Timestamp(System.currentTimeMillis()), task.input, task.output, null), spark)

  }
}

```

```scala
class RealTimeGpsBusExecutor(spark: SparkSession, task: EtlJobConfig) extends BaseTask[STD_GPS_BUS] {

  def filter(x: String): Boolean = {
    val arr = x.split(",")
    val c1 = StdUtils.allNotEmpty(arr.slice(2, 6))

    val time_tmp = arr(0) + StdUtils.fillZero(6, arr(1))
    //loc_time	定位日期	整型	10	否		unix10位时间戳
    val dataTime = DateTime.parse(time_tmp, DateTimeFormat.forPattern("yyyyMMddHHmmss"))

    //    val c3 = dataTime.getMillis <= DateTime.now().getMillis

    val today = DateTime.parse(task.input.substring(task.input.lastIndexOf("/") - 7), DateTimeFormat.forPattern("yyyy/MM/dd")).plusDays(1)

    c1 && dataTime.getMillis >= today.minusDays(1).getMillis && dataTime.getMillis < today.getMillis

  }

  def map(x: String, routeBC: Broadcast[Map[String, String]], vnoBC: Broadcast[Map[String, String]]) = {
    //    20191118,235958,H,53075,113.92556,22.669336,0,0,1,1,5668,0,20191118 23:59:58

    val arr = x.split(",")

    val time_tmp = arr(0) + StdUtils.fillZero(6, arr(1))

    //loc_time	定位日期/**/	整型	10	否		unix10位时间戳
    val dataTime = DateTime.parse(time_tmp, DateTimeFormat.forPattern("yyyyMMddHHmmss"))
    val loc_time = dataTime.getMillis / 1000

    //vehicle_id	车辆唯一标识（车牌）	字符串		否
    val vehicle_id = vnoBC.value.getOrElse(arr(3).trim, "-99")

    //lng	经度，小数点后取6位	浮点	10,6	否		WGS坐标系
    val lng = BigDecimal(arr(4).trim).setScale(6)

    //lat	纬度，小数点后取6位	浮点	10,6	否		WGS坐标系
    val lat = BigDecimal(arr(5).trim).setScale(6)

    //speed	瞬时速度（km/h）	浮点	4,1	否
    val speed = BigDecimal(arr(6).trim).setScale(1)

    //angle	方向角，与正北方向的顺时针夹角（0-359）	整型	3 	否
    val angle = arr(7).trim.toInt

    //company_code	所属公司代码	字符串		否		公司代码编码
    val company_code = arr(2)

    //busline_name	公交线路名称	字符串		否
    val busline_name = routeBC.value.getOrElse(arr(10).trim, "-99")


    // busline_dir	方向 	整型		是		0为上行，1为下行，2偏离路线，3为上行场区，4为下行场区
    val busline_dir = null

    //bus_station_order	站序	整型		是
    val bus_station_order = null

    //city	城市名称	字符串		否	1	参照城市编码对照表
    val city_name = "shenzhen"

    //深圳
    val city = 440300
    //    val city = "-99".toInt

    val is_validate = arr(9).toInt

    val bus_position = null

    val alarm_status: String = null
    val vehicle_status: String = null

    val year = arr(0).substring(0, 4).toInt
    val month = arr(0).substring(4, 6).toInt
    val day = arr(0).substring(6, 8).toInt


    STD_GPS_BUS(
      loc_time: Long,
      vehicle_id: String,
      lng: BigDecimal,
      lat: BigDecimal,
      speed: BigDecimal,
      angle: Int,
      is_validate: Int,
      company_code: String,
      busline_name: String,
      bus_position: Integer,
      busline_dir: Integer,
      bus_station_order: Integer,
      alarm_status: String,
      vehicle_status: String,
      city: Int,
      year: Int,
      month: Int,
      day: Int
    )
  }

  def execute() = {

    smartLoop(task.input: String, spark: SparkSession)
  }

  def process(dir: String) = {
    val rdd = hdfs2RDD(dir: String, spark: SparkSession, task: EtlJobConfig)
    import spark.implicits._
    val dimSource = new DimSource(spark)
    val routeBC = spark.sparkContext.broadcast(dimSource.getRouteId2NameMap())
    val vnoBC = spark.sparkContext.broadcast(dimSource.getVId2VnoMap())

    val res = spark.createDataset(rdd).as[String]
      .filter(x => filter(x))
      .map(x => map(x, routeBC, vnoBC))
      .filter(x =>
        x.vehicle_id.nonEmpty && x.lng != null && x.lat != null && StdUtils.isCnLngLat(x.lng.toDouble, x.lat.toDouble) && x.speed < 900
      )

    //    cache(res)

    TaskOutput.hive[STD_GPS_BUS](spark, task, Array("city", "year", "month", "day"), res, isPersist = false, dupliCols = Seq("vehicle_id", "loc_time"), isCopy2Tag = true)
    JobLogWriter.log(EtlJobLog(task.id, task.task_name, null, new Timestamp(System.currentTimeMillis()), task.input, task.output, null), spark)


  }
}

```



RealTimeGpsOrderExecutor
```scala
class RealTimeGpsOrderExecutor(spark: SparkSession, task: EtlJobConfig) extends BaseTask[STD_GPS_ORDER] {
  //20190531,235925,H,粤BDE4690,114.199640,22.647543,29.0,26,1,1,1,29,0,0,0,0,512

  //20191009,235959,WANSHUNJIAOCHE,B19EM6,114.050983,22.636803,32.0,359,0,1,9000042793,362531196310061216,1,63.0,3,XS,0


  //定位日期 定位时刻 公司代码 车牌号 经度 纬度 速度 方向角 运营状态 数据可用性 车辆颜色 行车速度 总里程 海拔高度 车辆状态 报警状态 车辆类型
  //20190531,235920,H,    粤BFX015,  115.910216,  23.003230,  0.0,  0, 1, 1, 2,  0,131673600,18,786434,262144,512

  def filter(x: String): Boolean = {
    val arr = x.split(",")
    val c1 = StdUtils.allNotEmpty(arr.slice(0, 7)) && arr.length == 17

    val time_tmp = arr(0) + StdUtils.fillZero(6, arr(1))
    //loc_time	定位日期	整型	10	否		unix10位时间戳
    val dataTime = DateTime.parse(time_tmp, DateTimeFormat.forPattern("yyyyMMddHHmmss"))

    val today = DateTime.parse(task.input.substring(task.input.lastIndexOf("/") - 7), DateTimeFormat.forPattern("yyyy/MM/dd")).plusDays(1)

    c1 && dataTime.getMillis >= today.minusDays(1).getMillis && dataTime.getMillis < today.getMillis

  }

  def map(x: String) = {
    val arr = x.replaceAll("null", "-99").split(",")


    val time_tmp = arr(0) + StdUtils.fillZero(6, arr(1))

    //loc_time	定位日期	整型	10	否		unix10位时间戳
    val dataTime = DateTime.parse(time_tmp, DateTimeFormat.forPattern("yyyyMMddHHmmss"))
    val loc_time = dataTime.getMillis / 1000

    //vehicle_id	车辆唯一标识（车牌）	字符串		否
    val vehicle_id = if (arr(3).startsWith("B")) "粤" + arr(3) else arr(3)

    //lng	经度，小数点后取6位	浮点	10,6	否		WGS坐标系
    val lng = BigDecimal(arr(4)).setScale(6, RoundingMode.HALF_UP)

    //lat	纬度，小数点后取6位	浮点	10,6	否		WGS坐标系
    val lat = BigDecimal(arr(5)).setScale(6, RoundingMode.HALF_UP)

    //speed	瞬时速度（km/h）	浮点	4,1	否
    val speed = BigDecimal(arr(6)).setScale(2, RoundingMode.HALF_UP)

    //angle	方向角，与正北方向的顺时针夹角（0-359）	整型	3 	否
    val angle = if (arr(7).isEmpty) -99 else arr(7).toInt

    //company_code	所属公司代码	字符串		否		公司代码编码
    val company_code = arr(2)


    //operation_status	营运状态	整型	1 	否		1-载客，2-接单，3-空驶，4-停运
    val operation_status = arr(8).toInt

    //order_id	订单id	字符串	20	否		订单编号
    val order_id = arr(16)

    //altitude	海拔高度，单位m	整型	2	否
    val altitude = if (arr(13).isEmpty) -99 else arr(13).toDouble.toInt

    //position_type	位置信息类型	字符串	10	否		JYSX-经营上线、JYXX经营下线、CKSC乘客上车、CKXC-乘客下车、DDJD-订单派单
    val position_type = arr(15)


    //city	城市名称	字符串		否	1	参照城市编码对照表
    val city_name = "shenzhen"

    //深圳
    val city = 440300

    val year: String = arr(0).substring(0, 4)
    val month: String = arr(0).substring(4, 6)
    val day: String = arr(0).substring(6, 8)

    STD_GPS_ORDER(
      loc_time: Long,
      vehicle_id: String,
      lng: BigDecimal,
      lat: BigDecimal,
      speed: BigDecimal,
      angle: Int,
      operation_status: Int,
      position_type: String,
      order_id: String,
      altitude: Int,
      company_code: String,
      city: Int,
      year: String,
      month: String,
      day: String
    )
  }

  def execute() = {
    println("读取【" + task.input + "】数据")
    println("数据格式：定位日期 定位时刻 用户ID 导航状态 经度 纬度 瞬时速度 方位角")
    println("20170930,235958,M338,粤BN6601,114.029805,22.670012,37,135,1,1")
    smartLoop(task.input: String, spark: SparkSession)
  }

  def process(dir: String) = {
    val rdd = hdfs2RDD(dir: String, spark: SparkSession, task: EtlJobConfig)
    import spark.implicits._
    val res = spark.createDataset(rdd).as[String]
      .filter(x => filter(x))
      .map(x => map(x)).filter(_.speed < 900)

    //    cache(res)

    TaskOutput.hive[STD_GPS_ORDER](spark, task, Array("city", "year", "month", "day"), res, isPersist = false, dupliCols = Seq("vehicle_id", "loc_time"), isCopy2Tag = true)
    JobLogWriter.log(EtlJobLog(task.id, task.task_name, null, new Timestamp(System.currentTimeMillis()), task.input, task.output, null), spark)

  }
}

```


## 定时调度

```scala
30 00 * * * nohup su - hdfs -c "spark-submit --master yarn  --deploy-mode client --class com.sutpc.bigdata.etl.EtlApp --files /etc/hbase/conf/hbase-site.xml --executor-cores 5 --num-executors 6  --executor-memory 5g --driver-memory 1g --conf spark.yarn.maxAppAttempts=4  --conf spark.task.maxFailures=8     --conf spark.yarn.am.attemptFailuresValidityInterval=1h --conf spark.yarn.executor.failuresValidityInterval=1h --conf spark.sql.adaptive.enabled=true    --conf spark.yarn.max.executor.failures=24 --conf spark.blacklist.enabled=false    --conf "spark.executor.extraJavaOptions=-XX:+UseG1GC"  ./jars_zyt/spark-sutpc-1.0-SNAPSHOT.jar" >>/root/spark-logs/etljob.log 2>&1  &


```
