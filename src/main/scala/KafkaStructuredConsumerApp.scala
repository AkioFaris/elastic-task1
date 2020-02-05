import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode

import scala.io.Source

object KafkaStructuredConsumerApp extends App {

  val url = getClass.getResource("config.properties")
  val properties: Properties = new Properties()
  properties.load(Source.fromURL(url).bufferedReader())

  val sparkSession = SparkSession.builder
    .appName("KafkaStructuredConsumerApp")
    .getOrCreate()
  sparkSession.sparkContext.setLogLevel("ERROR")

  val df = sparkSession
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", properties.getProperty("broker_host_port"))
    .option("subscribe", properties.getProperty("topic"))
    .option("checkpointLocation", properties.getProperty("checkpoint_path"))
    .option("failOnDataLoss", false)
    .option("startingOffsets", "earliest")
    .load()

  import sparkSession.implicits._

  val block_sz = 1024
  val query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .as[(String, String)]
    .map(pair => pair._2.replaceAll("\"", ""))
    .writeStream
    .outputMode(OutputMode.Append())
    .queryName("writing_to_es")
    .format("es")
    .option("checkpointLocation", "/tmp/checkpoint")
    .option("es.nodes", "127.0.0.1")
    .option("es.port", "9200")
    .option("es.resource", "test_elk/entity")
    .option("es.index.auto.create", "true")
    .start()

  while (query.isActive) {
    val msg = query.status.message
    if (!query.status.isDataAvailable && !query.status.isTriggerActive && !msg.equals("Initializing sources")) {
      query.stop()
    }
  }
  sparkSession.stop()
}
