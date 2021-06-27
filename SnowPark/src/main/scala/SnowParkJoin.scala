import com.snowflake.snowpark._
import com.snowflake.snowpark.SaveMode.Overwrite

import scala.tools.nsc.interpreter
import scala.tools.nsc.interpreter.session

// Imported for adjusting the logging level.
import org.apache.log4j.{Level, Logger}

/**
  * Connects to a Snowflake database and prints a list of tables in the database to the console.
  *
  * You can use this class to verify that you set the connection properties correctly in the
  * snowflake_connection.properties file that is used by this code to create a session.
  */
object SnowparkJoin {
  def main(args: Array[String]): Unit = {
    // By default, the library logs INFO level messages.
    // If you need to adjust the logging levels, uncomment the statement below, and change X to
    // the level that you want to use.
    // (https://logging.apache.org/log4j/1.2/apidocs/org/apache/log4j/Level.html)
    // Logger.getLogger("com.snowflake.snowpark").setLevel(Level.X)

    Console.println("\n=== Creating the session ===\n")
    // Create a Session that connects to a Snowflake deployment.

    val session = Session.builder.configFile("snowflake_connection.properties").create
    val reg = session.sql( query = "select * from region")
    val nat = session.sql( query = "select * from nation")
    val joined_df = reg.join(nat,reg("R_REGIONKEY") ===  nat("N_REGIONKEY"),"inner")
    joined_df.show()
    joined_df.write.mode(Overwrite).saveAsTable("joined_df")

  }
}