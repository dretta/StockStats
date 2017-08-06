package com.dretta.json

import java.text.SimpleDateFormat
import java.util.Date
import java.lang.{Double => JavaDouble, Integer => JavaInteger}
import play.api.libs.json.{JsString, JsValue}

trait GetJsValue {

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
