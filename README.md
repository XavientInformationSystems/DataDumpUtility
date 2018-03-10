# Data Dump Utility

This spark based command line driven utility can be used to fetch  and store data from various source and destination file systems including s3 , gs , hdfs
and local file type system. 
It can also be used to create a table in Amazon Athena if the destination data is S3


# Command Line Options available
  On top of the regular spark command line options , this utility provide switches to provide necessary information to retrieve and stored  the data from the specific  filesystem. These are 
  
  > Generic Options
 -  s    :  Source  Location
 -  d    : Destination Location . Default  to s + f
 -  f    : Destination data format . Defaults to ORC
 -  e    : External schema location . If not provided , the schema is created using the  source file headers
 
> S3 Related Options  
-   s3ak : Access Key for the  AWS System
-   s3sk :  Secret Key for the  AWS System


> Google Cloud Related Options
-   gsi :   Google Project Id
-   gss :   Service Account for the  GCS System
-   gsp : Path to the P12 file

>Athena  Related Optios
-   adb :   Athena Database
-   at :    Athena Table Name
-   as  :   Athena Staging Directory
-   act :   Create Table - true or false  .Defaults to false
-   acs :   Athena Conection String
-   p : Create Partitioned Data


# How to
 >  build application
 
 Unzip the project and perform a maven build in its root directory
 
 ```sh
 mvn clean package
 ```
 
 > use with the generic options
 
 ```sh
  spark-submit --class com.xavient.datadump.StoreData target/com.xavient.datadump.StoreData DataDumpUtility-0.0.1-SNAPSHOT-jar-with-dependencies.jar -s test.csv -f parquet -d destinationDirectory -e hdfs://<<pathToExternalSchema
```
 or can be used without the destination , format or the external schema
 ```sh
  spark-submit --class com.xavient.datadump.StoreData target/com.xavient.datadump.StoreData DataDumpUtility-0.0.1-SNAPSHOT-jar-with-dependencies.jar -s test.csv
```

>For S3 file type system 
```sh
spark-submit   --jars=AthenaJDBC41-1.0.0.jar --master yarn --class com.xavient.datadump.StoreData DataDumpUtility-0.0.1-SNAPSHOT-jar-with-dependencies.jar  -s clientdata   -d s3://<<bucketPath>>  -s3ak <<AccessKey>>  -s3sk <<SecretKey> 
```

With s3 as a destination system we can also create an athena table by passing athena related options. Athena  jar can be downloaded from [here](https://s3.amazonaws.com/athena-downloads/drivers/AthenaJDBC41-1.0.0.jar)

```sh
spark-submit   --jars=AthenaJDBC41-1.0.0.jar --master yarn --class com.xavient.datadump.StoreData DataDumpUtility-0.0.1-SNAPSHOT-jar-with-dependencies.jar  -s clientdata   -d s3://<<bucketPath>>  -s3ak <<AccessKey>>  -s3sk <<SecretKey>  -act true -at <<table_name>> -adb <<Existing_dbname_name>> -acs jdbc:awsathena://<<Athena URL>>:443/ -as s3://<<temp_bucketPath>> 
```

Table can also be created with partion using the "p" switch to true
```sh
spark-submit   --jars=AthenaJDBC41-1.0.0.jar --master yarn --class com.xavient.datadump.StoreData DataDumpUtility-0.0.1-SNAPSHOT-jar-with-dependencies.jar  -s clientdata   -d s3://<<bucketPath>>  -s3ak <<AccessKey>>  -s3sk <<SecretKey>  -act true -at finalTest -adb sampledb -acs jdbc:awsathena://<<Athena URL>>:443/ -as s3://<<temp_bucketPath>> -p true
```

If the table created is partioned then execute the following command to in the Athena console before viewing the data
```sh
MSCK REPAIR TABLE  <<dbname>>.<<tablename>>
```

> For Google Cloud File System

```sh
spark-submit --master yarn --class com.xavient.datadump.StoreData target/DataDumpUtility-0.0.1-SNAPSHOT-jar-with-dependencies.jar  -s clientdata   -d gs://<<destination>>  -gsi <<google project id >>  -gss <<google service account>> -gsp <<path to .p12 file >> -f parquet
```
