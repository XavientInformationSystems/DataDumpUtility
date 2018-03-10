

import java.sql.DriverManager
import java.util.Properties

object AthenaTest {

  def main(args: Array[String]): Unit = {
    val info = new Properties
    info.put("user", "UserIdPlease")
    info.put("password", "Passswrd Please")
    info.put("s3_staging_dir", "s3://faltubucket/DataFromDataDump/Test1")

    Class.forName("com.amazonaws.athena.jdbc.AthenaDriver")

    val connection = DriverManager.getConnection("jdbc:awsathena://athena.us-east-1.amazonaws.com:443/", info )

    val statement = connection.createStatement

    val sd = statement.executeQuery("select * from sampledb.elb_logs limit 2")

    sd.next()

    println(sd.getString(1))

  }

}
