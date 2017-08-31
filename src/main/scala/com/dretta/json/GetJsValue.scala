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

  def getDouble(jsDouble : JsValue) : JavaDouble = getNumber(jsDouble)(JavaDouble.valueOf)

  def getInteger(jsInteger : JsValue) : JavaInteger = getNumber(jsInteger)(JavaInteger.valueOf)

  def getNumber[T](jsNumber : JsValue)(f : String => T) : T = {
    val strNumber : String = jsNumber.asInstanceOf[JsString].value.replace(",", "")
    f(strNumber)
  }

  def getString(jsString : JsValue) : String = {
    jsString.asInstanceOf[JsString].value.replace("\"", "")
  }
}
