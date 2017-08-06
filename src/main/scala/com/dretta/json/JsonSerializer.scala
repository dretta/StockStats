package com.dretta.json

import java.io.UnsupportedEncodingException
import java.util

import _root_.kafka.serializer.Decoder
import _root_.kafka.utils.VerifiableProperties
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Serializer
import play.api.libs.json.{JsValue, Json}

class JsonSerializer extends Serializer[JsValue]{

  private val encoding = "UTF8"

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit={}

  override def close(): Unit={}

  def serialize(topic: String, data: JsValue): Array[Byte] = {
    val opData: Option[JsValue] = Option(data)
    try {
      opData.map(_.toString.getBytes(encoding)).orNull
    } catch {
      case e: UnsupportedEncodingException =>
        throw new SerializationException("Error when serializing JsValue (toString) to Array[Byte] " +
          "due to unsupported encoding " + encoding )
    }
  }

}


class JsonDecoder(props: VerifiableProperties = null) extends Decoder[JsValue]{

  val encoding =
    if(props == null)
      "UTF8"
    else
      props.getString("serializer.encoding", "UTF8")

  override def fromBytes(bytes: Array[Byte]): JsValue = {
    val opData: Option[Array[Byte]] = Option(bytes)
    try {
      opData.map(new String(_, encoding)).map(Json.parse).orNull
    } catch {
      case e: UnsupportedEncodingException =>
        throw new SerializationException("Error when deserializing Array[Byte] to (string) JsValue " +
          "due to unsupported encoding " + encoding )
    }
  }
}