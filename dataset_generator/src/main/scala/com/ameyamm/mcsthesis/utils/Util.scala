package com.ameyamm.mcsthesis.utils

import scala.collection.immutable.StringOps
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormatter
import org.joda.time.format.DateTimeFormat

/**
 * @author ameya
 */
object Util {
  def getSetFromString( setStringOption : Option[String] ) : Option[Set[String]] = {
    
    setStringOption match {
      case Some(setString) => { 
        val setPartOfString = setString.substring(setString.indexOf('[') + 1, setString.lastIndexOf(']'))
        Some(setPartOfString.split('|').map(_.trim).toSet) 
      } 
      case None => None
    }
  }
  
  def getBooleanFromString( booleanStringOption : Option[String] ) : Option[Boolean] = {
    booleanStringOption match {
      case Some(booleanString) => {
        if (booleanString.equalsIgnoreCase("f")) Some(true) 
        else Some(false) 
      }
      case None => None
    }
    
  }
  
  def getDateFromString( dateStringOption : Option[String] ) : Option[DateTime] = {
    dateStringOption match {
      case Some(dateString) => {
        val dateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
        Some(dateFormatter.parseDateTime(dateString))
      }
      case None => None
    }
  }
  
  def getStringFromOptionString( stringOption : Option[String] ) : String = {
    stringOption match {
      case Some(string) => string
      case None => "None"
    }
  }

}