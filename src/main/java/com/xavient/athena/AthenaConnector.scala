package com.xavient.athena

import java.sql.DriverManager
import java.util.Properties
import java.sql.Connection
import java.sql.Statement
import com.xavient.datadump.ApplicationArguments
import scala.collection.mutable.HashMap
import com.xavient.datadump.common.RDDToHiveDataTypes
import org.apache.spark.sql.types.StructType

class AthenaConnector {

  var connection: Connection = null
  var statement: Statement = null

  def init(appArgs: HashMap[String, String]) = {
    val info = new Properties
    println("reached here")
    info.put("user", appArgs.getOrElse("accessKey", ""))
    info.put("password", appArgs.getOrElse("secretKey", ""))

    info.put("s3_staging_dir", appArgs.getOrElse("athenaStagingDirectory", ""))

    Class.forName("com.amazonaws.athena.jdbc.AthenaDriver")

    this.connection = DriverManager.getConnection(appArgs.getOrElse("athenaConnectionString", ""), info)

  }

  def createTable(destinationFileSchema: StructType, appArgs: HashMap[String, String]) = {
    init(appArgs)

    statement = this.connection.createStatement

    statement.execute(createDDL(destinationFileSchema, appArgs))
    statement.close()
  }

  def createDDL(destinationFileSchema: StructType, appArgs: HashMap[String, String]): String =
    {
      val createTableCommand = new StringBuilder(" CREATE EXTERNAL  TABLE IF NOT EXISTS ")
      val hiveDataTypes = new RDDToHiveDataTypes().getHiveDataTypes()
      createTableCommand.append(appArgs.getOrElse("athenaDatabase", "")  +"." + appArgs.getOrElse("athenaTable", "")
          +" ( ")
      
      
      
      destinationFileSchema.foreach { x =>
        createTableCommand.append(x.name + " " + hiveDataTypes.getOrElse(x.dataType, "STRING") + " ,")
      }
      createTableCommand.deleteCharAt(createTableCommand.length - 1).append(")")
       
      createTableCommand.deleteCharAt(createTableCommand.length - 1).append(")") 
       if( appArgs.getOrElse("createPartition", "false") == "true"){
     createTableCommand.append( " PARTITIONED BY (year STRING  , month STRING  , date STRING ) ")
       }
      createTableCommand.append( " stored as " +
        appArgs.getOrElse("type", "orc") + " location '" + appArgs.getOrElse("destination", "") + "/'")

      createTableCommand.toString
    }

  def stop() {

    connection.close()
  }

}