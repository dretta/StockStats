package com.dretta

import com.datastax.spark.connector._
import _root_.kafka.serializer.StringDecoder
import java.util.{Date, Properties}
import java.text.SimpleDateFormat

import com.dretta.kafka.StockProducer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import play.api.libs.json._
import com.datastax.spark.connector.writer.WriteConf
import org.apache.spark.sql.cassandra._
import java.lang.{Double => JavaDouble, Integer => JavaInteger}

import org.apache.kafka.clients.producer
import org.apache.spark.{SparkConf, SparkContext}

object StockStats extends App {

    var events : Int = args(0).toInt
    val topic = args(1)
    val brokers = args(2)

    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    props.put("client.id", "ScalaProducerExample")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "com.dretta.JsonSerializer")

    val producer = new StockProducer(topic, brokers, props)

    val conf = new SparkConf().setMaster("local[*]").setAppName("StockStats")
    //conf.setJars(Seq("./target/scala-2.11/StockStats-assembly-1.0.jar"))

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

  def getDate(datetime : JsValue): Date = {
    val strDateTime : String = datetime.as[String]
    val format = new SimpleDateFormat("YYYY-MM-dd'T'HH:mm:ss'Z'")
    format.parse(strDateTime)
  }

  def getDouble(jsDouble : JsValue) : JavaDouble = {
    val strDouble : String = jsDouble.asInstanceOf[JsString].value.replace(",", "")
    val jDouble : JavaDouble = JavaDouble.valueOf(strDouble)
    jDouble
  }

  def getInteger(jsInteger : JsValue) : JavaInteger = {
    val strInteger : String = jsInteger.asInstanceOf[JsString].value
    val jInteger : JavaInteger = JavaInteger.valueOf(strInteger)
    jInteger
  }
//TODO: Make getNumber working
/*
  def getNumber[T](jsNumber : JsValue) : T = {
    val strNumber : String = jsNumber.asInstanceOf[JsString].value
    val jNumber = T  match {
      case JavaInteger => 0
      case JavaDouble => 0.0
    }
    jNumber
  }
*/
  def getString(jsString : JsValue) : String = {
    jsString.asInstanceOf[JsString].value.replace("\"", "")
  }

}

