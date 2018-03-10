package com.xavient.datadump.common

import scala.collection.mutable.HashMap
import org.apache.commons.cli.BasicParser
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.Options
import com.xavient.datadump.ApplicationArguments

class CommandLineOptions {

  var commandLine: CommandLine = null;

  def validateArguments(args: Array[String]): HashMap[String, String] = {

    val parser = new BasicParser
    commandLine = parser.parse(getOptions(), args)

    new ApplicationArguments().initializeApplicationArguments(commandLine)

  }

  def getOptions(): Options = {
    val options = new Options();
    //generic option
    options.addOption("s", "sourceFile", true, "Source  file location")
    options.addOption("d", "destinationFile", true, "Destination  file location")
    options.addOption("f", "format", true, "Destination  file format")
    options.addOption("e", "externalSchema", true, "Destination  file Schema")

    //s3 related options
    options.addOption("s3ak", "s3AccessKey", true, "Access Key for the  AWS System")
    options.addOption("s3sk", "s3SecretKey", true, "Secret Key for the  AWS System")

    //google cloud related options
    options.addOption("gsi", "gcsProjectId", true, "Google Project Id")
    options.addOption("gss", "gcsServiceAccount", true, "Service Account for the  GCS System")
    options.addOption("gsp", "gcsP12Path", true, "Path to the P12 file")
    
    //athena  related optios
    options.addOption("adb", "athenaDatabase", true, "Athena Database")
    options.addOption("at", "athenaTable", true, "Athena Table Name ")
    options.addOption("as", "athenaStagingDirectory", true, "Athena Staging Directory")
    options.addOption("act", "athenaCreateTable", true, "Create Table - true or false")
    options.addOption("acs", "athenaConnectionString", true, "Athena Conection String")
    
    options.addOption("p" , "createPartition" , true , "Create Partitioned Data" )
    
    return options;
    
  }
  

}



