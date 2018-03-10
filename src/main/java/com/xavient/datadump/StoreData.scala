package com.xavient.datadump

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import com.xavient.datadump.common.CommandLineOptions
import org.apache.hadoop.fs.s3.S3FileSystem
import org.apache.spark.sql.types.StructType
import com.xavient.datadump.common.RDDToHiveDataTypes
import com.xavient.athena.AthenaConnector
import com.xavient.datadump.common.DateExtractor
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._;
import java.text.SimpleDateFormat
import java.text.DateFormat
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DataTypes

object StoreData {

  def main(args: Array[String]): Unit = {

    val applicationArguments = new CommandLineOptions().validateArguments(args)
    val sourceFile = applicationArguments.getOrElse("source", "")
    val conf = new SparkConf().setAppName("DataDumpJob").setMaster("local[*]")
    val context = new SparkContext(conf)

    val destination = applicationArguments.getOrElse("destination", "")

    if (destination.startsWith("""s3://""")) {
      context.hadoopConfiguration.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
      context.hadoopConfiguration.set("fs.s3.awsAccessKeyId", applicationArguments.getOrElse("accessKey", ""))
      context.hadoopConfiguration.set("fs.s3.awsSecretAccessKey", applicationArguments.getOrElse("secretKey", ""))
    } else if (destination.startsWith("""gs://""")) {
      context.hadoopConfiguration.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
      context.hadoopConfiguration.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

      context.hadoopConfiguration.set("fs.gs.project.id", applicationArguments.getOrElse("gcsProjectId", ""))
      context.hadoopConfiguration.set("google.cloud.auth.service.account.enable", "true")
      context.hadoopConfiguration.set("google.cloud.auth.service.account.email", applicationArguments.getOrElse("gcsServiceAccount", "gcsServiceAccount"))
      context.hadoopConfiguration.set("google.cloud.auth.service.account.keyfile", applicationArguments.getOrElse("gcsP12Path", "/etc/hadoop/conf/googleKey.p12"))
    }

    context.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    context.hadoopConfiguration.set("parOquet.enable.summary-metadata", "false")

    val hiveContext: HiveContext = new HiveContext(context);

    var destinationFileSchema: StructType = null

    if (applicationArguments.getOrElse("schemaAvailable", "false") == "true") {

      destinationFileSchema = new PopulateSchema().creatDestinationSchema(applicationArguments.getOrElse("schemaPath", ""))

    } else {

      destinationFileSchema = new PopulateSchema().getSchemaFromSource(hiveContext, sourceFile)

    }

    var dfNew = hiveContext.read.format("com.databricks.spark.csv").option("inferSchema", "false")
      .option("header", "true").option("delimiter", ",").schema(destinationFileSchema).load(sourceFile)

if(applicationArguments.getOrElse("createPartition", "false") == "true") {
     val dateExtractor = new DateExtractor
    import hiveContext.implicits._

    val newColumns = dfNew.columns

    val newCoumns2: Array[String] = Array.ofDim[String](dfNew.columns.size + 3)

    for (index <- 0.until(dfNew.columns.size)) {
      newCoumns2(index) = newColumns(index)
    }

    newCoumns2(newColumns.size) = "year"
    newCoumns2(newColumns.size + 1) = "month"
    newCoumns2(newColumns.size + 2) = "date"

    val extractyear = udf(dateExtractor.extractyear _)
    val extractmonth = udf(dateExtractor.extractmonth _)
    val extractdate = udf(dateExtractor.extractydate _)

     dfNew = dfNew.withColumn("year", extractyear(dfNew.col(newColumns(0))))
    dfNew = dfNew.withColumn("month", extractmonth(dfNew.col(newColumns(0))))
    dfNew = dfNew.withColumn("date", extractdate(dfNew.col(newColumns(0))))

   
     
    dfNew.write.format(applicationArguments.getOrElse("type", "orc").toString())
      .partitionBy("year", "month", "date").save(destination);
}
else{
  dfNew.write.format(applicationArguments.getOrElse("type", "orc").toString()).save(destination);
}
 
 if (destination.startsWith("""s3://""") && applicationArguments.getOrElse("athenaCreateTable", "false") == "true") {

      new AthenaConnector().createTable(destinationFileSchema, applicationArguments)

    }

  }

  def createSparkContext(conf: SparkConf, destination: String) = {

  }

}