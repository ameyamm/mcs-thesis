package com.ameyamm.mcsthesis.utils

import java.util.Date
import java.text.SimpleDateFormat

/**
 * @author ameya
 */
object Utils extends Serializable{
  def removeAtIdx[T](list : List[T], index : Int ) : List[T] = {
    val (left, right) = list.splitAt(index)
    left ++ right.tail
  }
  
  def convertToDate(dateStr : String) : Date = {
    if (dateStr == null)
      null
    else {
      val dateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
      dateformat.parse(dateStr)
    }
  }
}