package com.dretta

import play.api.libs.json.{JsValue, Json}

object StockStats extends App {
  var events = args(0).toInt
  val topic = args(1)
  val brokers = args(2)
  /*
  val props = new Properties()
  props.put("bootstrap.servers", brokers)
  props.put("client.id", "ScalaProducerExample")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new StockProducer(topic, brokers, props)

  val spark = SparkSession.builder().master("local[2]").getOrCreate()
  val sc = spark.sparkContext
  val ssc = new StreamingContext(sc, Seconds(1))
  val topicSet = topic.split(",").toSet
  val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
  val directKafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,kafkaParams,topicSet)

  val parsers = directKafkaStream.map(v => v)
  parsers.print()

  ssc.start()
  val endTime = System.currentTimeMillis() + (5 * 1000)

  while(System.currentTimeMillis() < endTime){
    producer.generateMessage()
    Thread.sleep(1000)
  }

  ssc.stop()
  producer.close()
  */
  val url = "https://www.google.com/finance/info?q=NASDAQ:GOOG"
  val result = scala.io.Source.fromURL(url).mkString
  val form = result.replaceAll("\n","").slice(3,result.length)
  val json: JsValue = Json.parse(form)
  //json(0)("t") == "GOOG"
  json.toString()
}
