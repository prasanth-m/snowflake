import com.snowflake.snowpark._
import net.snowflake.client.jdbc.internal.org.checkerframework.checker.units.qual.s
import java.net.URL
import java.io.File
import org.apache.commons.io.FileUtils

/**
  * Connects to a Snowflake database and prints a list of tables in the database to the console.
  *
  * You can use this class to verify that you set the connection properties correctly in the
  * snowflake_connection.properties file that is used by this code to create a session.
  */
object OpenApi {
  def main(args: Array[String]): Unit = {
    // By default, the library logs INFO level messages.
    // If you need to adjust the logging levels, uncomment the statement below, and change X to
    // the level that you want to use.
    // (https://logging.apache.org/log4j/1.2/apidocs/org/apache/log4j/Level.html)
    // Logger.getLogger("com.snowflake.snowpark").setLevel(Level.X)

    Console.println("\n=== Creating the session ===\n")
    // Create a Session that connects to a Snowflake deployment.

    val session = Session.builder.configFile("snowflake_connection.properties").create

    def getJsonData =  {
      val url = "https://data.cityofnewyork.us/resource/2npr-yv2b.json"
      scala.io.Source.fromURL(url).mkString
    }

    val df = session.createDataFrame(Seq(getJsonData))
    df.show()

    val dataStageName = "snowpark_demo_db_stage"
    Console.println(s"\n=== Creating the stage @$dataStageName ===\n")
    session.sql(s"create or replace stage $dataStageName").collect()

    val uploadDirUrl = "file://" + System.getProperty("user.dir").replace("\\", "/") + "/files_to_upload"
    val filePattern = "test.json"
    val PATH = "C:\\sfguide-snowpark-demo\\files_to_upload\\"
    val FILENAME = "data.json"

    FileUtils.copyURLToFile(new URL("https://health.data.ny.gov/api/views/jxy9-yhdk/rows.json"), new File(s"${PATH}/$FILENAME"))

    val res = session.file.put(s"${PATH}/$FILENAME",dataStageName)
    res.foreach(r => Console.println(s"  ${r.sourceFileName}: ${r.status}"))

    session.sql(s"ls @$dataStageName").show()
  }
}