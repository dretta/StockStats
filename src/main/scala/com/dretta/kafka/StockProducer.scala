package com.dretta.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer => Producer, ProducerRecord => Record}
import play.api.libs.json.{JsValue, Json}


class StockProducer(val topic: String, val brokers: String, props: Properties)
  extends Producer(props) {

  var totalEvents: Int = 0
  var totalSeconds: Long = 0
  val producer = new Producer[String, JsValue](props)

  def generateMessage(): Unit ={
    val url = "https://www.google.com/finance/info?q=NASDAQ:GOOG"
    val result = scala.io.Source.fromURL(url).mkString
    val form = result.replaceAll("\n","").slice(3,result.length)
    val parsed: JsValue = Json.parse(form)
    val stockMap = Map("symbol" -> parsed(0)("t"), "last_trade_date_time" -> parsed(0)("lt_dts"),
      "change_percent" -> parsed(0)("cp"), "change_price" -> parsed(0)("c"),
      "last_close_price" -> parsed(0)("pcls_fix"), "last_trade_price" -> parsed(0)("l"),
      "last_trade_size" -> parsed(0)("s"), "stock_index" -> parsed(0)("e"))
    val stockJson: JsValue = Json.toJson(stockMap)
    val data = new Record[String, JsValue](topic, stockJson)
    producer.send(data)
  }

}