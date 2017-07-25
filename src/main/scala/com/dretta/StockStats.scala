package com.dretta

import _root_.kafka.serializer.{DefaultDecoder, StringDecoder}
import java.util.Properties

import com.dretta.kafka.StockProducer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StockStats extends App with JsonDeserializer{
  var events = args(0).toInt
  val topic = args(1)
  val brokers = args(2)

  val props = new Properties()
  props.put("bootstrap.servers", brokers)
  props.put("client.id", "ScalaProducerExample")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")


  val producer = new StockProducer(topic, brokers, props)

  val spark = SparkSession.builder().master("local[2]").getOrCreate()
  val sc = spark.sparkContext
  val ssc = new StreamingContext(sc, Seconds(1))
  val topicSet = topic.split(",").toSet
  val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
  val directKafkaStream = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](ssc,kafkaParams,topicSet)

  val parsers = directKafkaStream.map(v => (v._1, deserialize(topic, v._2)))
  parsers.print()

  ssc.start()
  val endTime = System.currentTimeMillis() + (5 * 1000)

  while(System.currentTimeMillis() < endTime){
    producer.generateMessage()
    Thread.sleep(1000)
  }

  ssc.stop()
  producer.close()

}

