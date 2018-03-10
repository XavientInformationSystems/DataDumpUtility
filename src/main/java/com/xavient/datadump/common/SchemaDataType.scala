package com.xavient.datadump.common

import scala.collection.mutable.HashMap
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DataTypes

class SchemaDataType {

  def initializeType: HashMap[String, DataType] = {
    val rddDataTypes = new HashMap[String, DataType]
    rddDataTypes.put("string", DataTypes.StringType)
    rddDataTypes.put("integer", DataTypes.IntegerType)
    rddDataTypes.put("byte", DataTypes.ByteType)
    rddDataTypes.put("calendarinterval", DataTypes.CalendarIntervalType)
    rddDataTypes.put("binary", DataTypes.BinaryType)
    rddDataTypes.put("boolean", DataTypes.BooleanType)
    rddDataTypes.put("date", DataTypes.DateType)
    rddDataTypes.put("double", DataTypes.DoubleType)
    rddDataTypes.put("long", DataTypes.LongType)
    rddDataTypes.put("float", DataTypes.FloatType)
    rddDataTypes.put("null", DataTypes.NullType)
    rddDataTypes.put("short", DataTypes.ShortType)

    rddDataTypes

  }

}