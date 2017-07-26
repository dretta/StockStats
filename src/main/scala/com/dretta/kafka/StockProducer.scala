package com.dretta.kafka

import java.util.{Date, Properties}

import org.apache.kafka.clients.producer.{KafkaProducer => Producer, ProducerRecord => Record}
import play.api.libs.json.{JsValue, Json}
import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import com.dretta.JsonSerializer

import scala.util.Random

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
    //println(stockMap.toString())
    val stockJson: JsValue = Json.toJson(stockMap)
    //val data = new Record[String, Array[Byte]](topic, serialize(topic, stockJson))
    val data = new Record[String, JsValue](topic, stockJson)
    //totalEvents += 1
    producer.send(data)
  }
  /*
  def generateMessages(events: Int){
    val rnd = new Random()
    val t = System.currentTimeMillis()
    for (nEvents <- Range(0, events)) {
      val runtime = new Date().getTime
      val ip = "192.168.2." + rnd.nextInt(255)
      val msg = runtime + "," + nEvents + ",www.example.com," + ip
      val data = new Record[String, String](topic, ip, msg)

      producer.send(data)
    }
    totalSeconds += System.currentTimeMillis() - t
    totalEvents += events
    System.out.println("Sent out " + events + " message(s)")
  }
  */
  def speed() {
    System.out.println("Average messages sent per second: " + totalEvents * 1000 / totalSeconds)
  }
}