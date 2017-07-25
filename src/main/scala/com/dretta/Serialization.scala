// https://gist.github.com/ErunamoJAZZ/ce0e31a98ed9240e2efaa9a3135684bb
package com.dretta

import java.io.UnsupportedEncodingException
import java.util

import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import play.api.libs.json.{JsValue, Json}


class JsonSerializer extends Serializer[JsValue]{
  private val encoding = "UTF8"

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}
  override def close(): Unit = {}

  override def serialize(topic: String, data: JsValue): Array[Byte] = {
    val opData: Option[JsValue] = Option(data)
    try {
      opData.map(_.toString.getBytes(encoding)).orNull
    } catch {
      case e: UnsupportedEncodingException =>
        throw new SerializationException("Error when serializing JsValue (toString) to Array[Byte]" +
          "due to unsupported encoding " + encoding)
    }
  }
}

class JsonDeserializer extends Deserializer[JsValue]{
  private val encoding = "UTF8"

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}
  override def close(): Unit = {}

  override def deserialize(topic:String, data: Array[Byte]): JsValue = {
    val opData: Option[Array[Byte]] = Option(data)
    try {
      opData.map(new String(_, encoding)).map(Json.parse).orNull
    } catch {
      case e: UnsupportedEncodingException =>
        throw new SerializationException("Error when serializing JsValue (toString) to Array[Byte]" +
          "due to unsupported encoding " + encoding)
    }
  }
}
