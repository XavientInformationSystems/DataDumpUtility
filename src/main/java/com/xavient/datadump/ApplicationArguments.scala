package com.xavient.datadump

import scala.collection.mutable.HashMap
import org.apache.commons.cli.CommandLine
import org.apache.log4j.Logger;

class ApplicationArguments {

  val appArgs = new HashMap[String, String]

  val log: Logger = Logger.getLogger(classOf[ApplicationArguments].getName)
  def initializeApplicationArguments(commandLine: CommandLine): HashMap[String, String] =
    {

      addGenericOptions(commandLine)

      val destination = appArgs.getOrElse("destination", "")
      val source = appArgs.getOrElse("source", "")

      if (destination.startsWith("""s3://""") || source.startsWith("""s3://""")) {
        addS3Options(commandLine)
      } else if (destination.startsWith("""gs://""") || source.startsWith("""gs://""")) {
        addS3Options(commandLine)
      }

      return appArgs;

    }

  def addGSOptions(commandLine: CommandLine) =
    {

      if (commandLine.getOptionValue("gsi") == null || commandLine.getOptionValue("gss") == null ||
        commandLine.getOptionValue("gsp") == null) {
        log.error("GCS project id or service account  or  sercret key path is not provided")
        throw new Exception("GCS project id or service account  or  sercret key path is not provided")
      }
      appArgs.put("gcsProjectId", commandLine.getOptionValue("gsi"))
      appArgs.put("gcsServiceAccount", commandLine.getOptionValue("gss"))
      appArgs.put("gcsP12Path", commandLine.getOptionValue("gsp"))

    }

  def addS3Options(commandLine: CommandLine) =
    {

      if (commandLine.getOptionValue("s3ak") == null || commandLine.getOptionValue("s3sk") == null) {
        log.error("S3 access key or  sercret key is not provided")
        throw new Exception("S3 access key or  sercret key is not provided")
      }
      appArgs.put("accessKey", commandLine.getOptionValue("s3ak"))
      appArgs.put("secretKey", commandLine.getOptionValue("s3sk"))

      if (commandLine.getOptionValue("act") == "true") {
        addAthenaOptions(commandLine)
      }

    }

  def addAthenaOptions(commandLine: CommandLine) = {

    if (commandLine.getOptionValue("adb") == null || commandLine.getOptionValue("at") == null ||
      commandLine.getOptionValue("acs") == null || commandLine.getOptionValue("as") == null)
      throw new Exception(" Mandatory   Athena paramteres were not provided")

    val stagingDirectory = appArgs.getOrElse("athenaStagingDirectory", "_temp")

    appArgs.put("athenaDatabase", commandLine.getOptionValue("adb"))
    appArgs.put("athenaTable", commandLine.getOptionValue("at"))
    appArgs.put("athenaStagingDirectory", commandLine.getOptionValue("as"))
    appArgs.put("athenaCreateTable", commandLine.getOptionValue("act"))
    appArgs.put("athenaConnectionString", commandLine.getOptionValue("acs"))

  }

  def addGenericOptions(commandLine: CommandLine) = {

    val destinationFileType =
      if (commandLine.getOptionValue("f") != null) {
        commandLine.getOptionValue("f")
      } else {
        log.warn("No output format specified . Defaulting to ORC")

        "orc"
      }
    appArgs.put("type", destinationFileType)

    val destination = commandLine.getOptionValue("d")
    if (destination != null) {
      appArgs.put("destination", if (destination.endsWith("/")) destination.substring(0, destination.length() - 1) else destination)
    } else {

      appArgs.put("destination", commandLine.getOptionValue("s").concat(".") concat (destinationFileType))
      log.warn("No destination  specified . Default to " + commandLine.getOptionValue("s").concat(".") concat (destinationFileType))
    }

    if (commandLine.getOptionValue("e") != null) {

      appArgs.put("schemaAvailable", "true")
      appArgs.put("schemaPath", commandLine.getOptionValue("e"))
    } else {
      appArgs.put("schemaAvailable", "false")
      log.warn("No schema  specified . Creating from the source file")
    }
    appArgs.put("createPartition", commandLine.getOptionValue("p"))
    appArgs.put("source", commandLine.getOptionValue("s"))
  }

}