package com.xavient.datadump

import java.io.FileReader
import java.util.ArrayList
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.json.simple.JSONArray
import org.json.simple.JSONObject
import org.json.simple.parser.JSONParser

import com.xavient.datadump.common.CheckFieldName
import com.xavient.datadump.common.SchemaDataType

class PopulateSchema {

  def creatDestinationSchema(schemaLocation: String): StructType = {

    val schema = readDestinationSchema(schemaLocation)

    val fields = new ArrayList[StructField]
    val i = schema.iterator()
    val datatypes = new SchemaDataType().initializeType
    while (i.hasNext()) {

      val innerObj = i.next.asInstanceOf[JSONObject]

      datatypes.getOrElse(innerObj.get("type").asInstanceOf[String], DataTypes.StringType)

      fields.add(DataTypes.createStructField(innerObj.get("name").asInstanceOf[String], datatypes.getOrElse(innerObj.get("type").asInstanceOf[String], DataTypes.StringType), true))

    }
    return DataTypes.createStructType(fields);
  }

  def readDestinationSchema(schemaLocation: String): JSONArray =
    {

      val reader: FileReader = new FileReader(schemaLocation)
      val jsonParser = new JSONParser()
      val jsonObject: JSONObject = jsonParser.parse(reader).asInstanceOf[JSONObject]
      val lang: JSONArray = jsonObject.get("fields").asInstanceOf[JSONArray]
      return lang

    }

  def getSchemaFromSource(hiveContext: HiveContext, sourceFile: String): StructType = {

    val tableDDL = new StringBuilder("CREATE EXTERNAL TABLE IF NOT EXISTS sampledb.parquetTable (  ")

    val originalSchema = hiveContext.read.format("com.databricks.spark.csv").option("inferSchema", "true")
      .option("header", "true").option("delimiter", ",").load(sourceFile).schema

    val fields = new ArrayList[StructField]

    originalSchema.foreach { singlefield =>
      fields.add(DataTypes.createStructField(new CheckFieldName().getValidNewFieldName(singlefield.name), singlefield.dataType, true))
      

    }

    return DataTypes.createStructType(fields)
  }

}