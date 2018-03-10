package com.xavient.datadump.common

import java.text.DateFormat
import java.text.SimpleDateFormat
import java.util.Date

import scala.util.matching.Regex
import org.apache.spark.sql.Column

class DateExtractor extends Serializable {



  
 
  def extractyear(date: String): String = {
 
    
    val time = new Date(date.substring(0, date.length()-3).toLong)
    val formatter: DateFormat = new SimpleDateFormat("yyyy:dd:MM");

    val dateFormatted = formatter.format(time);

    
    return dateFormatted.split(":")(0)
  }
  
  def extractydate(date: String): String = {
    val time = new Date(date.substring(0, date.length()-3).toLong)
    val formatter: DateFormat = new SimpleDateFormat("yyyy:dd:MM");

    val dateFormatted = formatter.format(time);

    
    return dateFormatted.split(":")(1)
  }
    def extractmonth(date: String): String = {

    val time = new Date(date.substring(0, date.length()-3).toLong)
    val formatter: DateFormat = new SimpleDateFormat("yyyy:dd:MM");

    val dateFormatted = formatter.format(time);

    
    return dateFormatted.split(":")(2)
    }
    
    
         
    
}