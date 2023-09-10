import org.apache.log4j.Logger
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema
import org.apache.flink.connector.kafka.sink.KafkaSink
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import scala.collection.JavaConversions._
import com.datastax.spark.connector._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.cassandra._
import org.apache.spark._
import org.apache.spark.sql._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import org.apache.spark.sql._
import java.util
import java.util.Properties


case class SensorData(sensor_id: Int, canal_id: Int, level: Int)

object main extends App {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  //Feature built for Code4GovtTech by Siddharth Banga
  val spark = SparkSession.builder()
      .master("local[3]")
      .appName("enrichment")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.sql.shuffle.partitions", 2)
      .config("spark.cassandra.connection.host", "localhost")
      .config("spark.cassandra.connection.port", "9042")
      .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")
      .config("spark.sql.catalog.lh", "com.datastax.spark.connector.datasource.CassandraCatalog")
      .getOrCreate()

  val sensorsDb = spark.read
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace", "canals_db")
      .option("table", "sensors")
      .load()

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val kafkaSource = KafkaSource.builder()
  .setBootstrapServers("localhost:9092")
  .setTopics("flinkex")
  .setGroupId("flink-consumer-group")
  .setStartingOffsets(OffsetsInitializer.latest())
  .setValueOnlyDeserializer(new SimpleStringSchema())
  .build()

  val stream2 = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")

  val schema = StructType(Seq(
    StructField("sensor", IntegerType, nullable = false),
    StructField("canal", IntegerType, nullable = false),
    StructField("intensity", IntegerType, nullable = false)
  ))

  val stream1: DataFrame = stream2.map(x => {
    val arr = x.split(",")
    val sensor = arr(0).toInt
    val canal = arr(1).toInt
    val intensity = arr(2).toInt
    println(arr)
    println(sensor + " " + canal+ " " + intensity)
    val data = Seq((sensor, canal, intensity))
    val rows = data.map { case (sensor, canal, intensity) => Row(sensor, canal, intensity) }
    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
    df.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "canals_db", "table" -> "sensors")) 
      .mode("overwrite")
      .save()
  }
  )
  env.execute("main")

}

