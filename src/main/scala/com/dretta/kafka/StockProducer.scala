package com.dretta.kafka

import java.util.{Date, Properties}


import org.apache.kafka.clients.producer.{KafkaProducer => Producer, ProducerRecord => Record}

import scala.util.Random

class StockProducer(val topic: String, val brokers: String, props: Properties)
  extends Producer(props) {

  var totalEvents: Int = 0
  var totalSeconds: Long = 0
  val producer = new Producer[String, String](props)

  def generateMessage(): Unit ={
    val url = "https://www.google.com/finance/info?q=NASDAQ:GOOG"
    val data = new Record[String, String](topic, System.currentTimeMillis().toString)
    producer.send(data)
  }

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

  def speed() {
    System.out.println("Average messages sent per second: " + totalEvents * 1000 / totalSeconds)
  }
}