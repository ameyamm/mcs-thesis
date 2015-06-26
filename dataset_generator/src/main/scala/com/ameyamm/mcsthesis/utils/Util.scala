package com.ameyamm.mcsthesis.utils

import scala.collection.immutable.StringOps
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormatter
import org.joda.time.format.DateTimeFormat

/**
 * @author ameya
 */
object Util {
  def getSetFromString( setString : String ) : Set[String] = {
    val setPartOfString = setString.substring(setString.indexOf('[') + 1, setString.lastIndexOf(']'))
    setPartOfString.split('|').map(_.trim).toSet 
  }
  
  def getBooleanFromString( booleanString : String ) : Boolean = {
    if (booleanString.equalsIgnoreCase("f")) true 
    else false 
  }
  
  def getDateFromString( dateString : String ) : DateTime = {
    val dateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
    dateFormatter.parseDateTime(dateString)
  }
}