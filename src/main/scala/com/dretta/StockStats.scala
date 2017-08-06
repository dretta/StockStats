package com.dretta

import com.datastax.spark.connector._
import _root_.kafka.serializer.StringDecoder
import java.util.Properties
import com.dretta.kafka.StockProducer
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import play.api.libs.json._
import com.datastax.spark.connector.writer.WriteConf
import com.dretta.json.{GetJsValue, JsonDecoder}
import org.apache.spark.{SparkConf, SparkContext}

object StockStats extends App with GetJsValue{

    val topic = args(0)
    val brokers = args(1)

    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    props.put("client.id", "ScalaProducerExample")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "com.dretta.json.JsonSerializer")

    val producer = new StockProducer(topic, brokers, props)

    val conf = new SparkConf().setMaster("local[*]").setAppName("StockStats")

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(2))
    val topicSet = topic.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val directKafkaStream = KafkaUtils.createDirectStream[String, JsValue, StringDecoder, JsonDecoder](ssc,kafkaParams,topicSet)

    val parsers : DStream[JsValue] = directKafkaStream.map(v => v._2)
    parsers.print()

    parsers.foreachRDD(_.map{jsValue => (getString(jsValue("symbol")),
      getDate(jsValue("last_trade_date_time")),
      getDouble(jsValue("change_percent")),
      getDouble(jsValue("change_price")),
      getDouble(jsValue("last_close_price")),
      getDouble(jsValue("last_trade_price")),
      getInteger(jsValue("last_trade_size")),
      getString(jsValue("stock_index"))
      )}.saveToCassandra("ks", "stocks",
      writeConf = new WriteConf(ifNotExists = true)))

    ssc.start()

    val startTime = System.currentTimeMillis()
    val endTime = startTime + (10 * 1000)

    while(System.currentTimeMillis() < endTime){
      producer.generateMessage()
      Thread.sleep(2000)
    }

    val rdd = sc.cassandraTable("ks", "stocks")


    ssc.stop(stopSparkContext = false)
    producer.close()

    println("Database has " + rdd.count().toString + " entries")
    println("Symbols: " + rdd.map(_.getString("symbol")).take(3).mkString(", "))

    sc.stop()

}

