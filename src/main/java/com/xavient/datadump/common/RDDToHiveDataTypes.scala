package com.xavient.datadump.common

import scala.collection.mutable.HashMap
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DataTypes

class RDDToHiveDataTypes {
  
  
  
  def getHiveDataTypes() : HashMap[DataType , String  ] =
  {
    var hiveDataTypes = new HashMap[ DataType ,String]  
    hiveDataTypes.put( DataTypes.StringType , "STRING")
    hiveDataTypes.put( DataTypes.IntegerType , "INT")
    hiveDataTypes.put(DataTypes.ByteType, "STRING")
    hiveDataTypes.put( DataTypes.CalendarIntervalType , "STRING")
    hiveDataTypes.put( DataTypes.BinaryType ,"STRING")
    hiveDataTypes.put( DataTypes.BooleanType , "BOOLEAN")
    hiveDataTypes.put( DataTypes.DateType , "DATE")
    hiveDataTypes.put( DataTypes.DoubleType , "DOUBLE")
    hiveDataTypes.put( DataTypes.LongType , "BIGINT" )
    hiveDataTypes.put( DataTypes.FloatType, "FLOAT")
    hiveDataTypes.put( DataTypes.NullType , "STRING")
    hiveDataTypes.put( DataTypes.ShortType , "INT")
    
    hiveDataTypes
  }
}