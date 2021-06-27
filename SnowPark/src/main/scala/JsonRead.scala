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
object JsonRead {
  def main(args: Array[String]): Unit = {
    // By default, the library logs INFO level messages.
    // If you need to adjust the logging levels, uncomment the statement below, and change X to
    // the level that you want to use.
    // (https://logging.apache.org/log4j/1.2/apidocs/org/apache/log4j/Level.html)
    // Logger.getLogger("com.snowflake.snowpark").setLevel(Level.X)

    Console.println("\n=== Creating the session ===\n")
    // Create a Session that connects to a Snowflake deployment.

    val session = Session.builder.configFile("snowflake_connection.properties").create
    val filePath = "@SNOWPARK_DEMO_DB_STAGE"
    val jsonDF = session.read.option("compression", "gzip").json(filePath)
    // Load the data into the DataFrame and return an Array of Rows containing the results.
    val results = jsonDF.collect()
  }
}