import com.snowflake.snowpark._
import com.snowflake.snowpark.functions._

/**
  * Sets up the files needed for the Snowpark demo of user-defined functions (UDFs).
  *
  * After downloading the data and JAR files needed by the UDF, use this class to create internal
  * stages and upload these files to those stages.
  */
object UploadData {

  // The session for connecting to Snowflake
  var session: Session = null

  // The file URL to the current working directory, where you copied the data and JAR files.
  val uploadDirUrl = "file://" + System.getProperty("user.dir").replace("\\", "/") + "/files_to_upload"

  // The name of the internal stage for the JAR files needed by the UDF.
  val jarStageName  = "snowpark_demo_db_dependency_jars"


  // The pattern matching the JAR files to upload.
  val jarFilePattern = "*.jar"

  // Creates the specified stage and uploads files matching the specified pattern to the stage.
  def uploadDemoFiles(stageName: String, filePattern: String): Unit = {
    Console.println(s"\n=== Creating the stage @$stageName ===\n")
    // Create an internal named stage. The collect() method executes the statement.
    session.sql(s"create or replace stage $stageName").collect()

    Console.println(s"\n=== Uploading files matching $filePattern to @$stageName ===\n")
    // Upload the files matching the pattern to the stage.
    val res = session.file.put(s"${uploadDirUrl}/$filePattern", stageName)
    res.foreach(r => Console.println(s"  ${r.sourceFileName}: ${r.status}"))

    Console.println(s"\n=== Files in @$stageName ===\n")
    // List the files in the stage.
    session.sql(s"ls @$stageName").show()
  }

    session = Session.builder.configFile("snowflake_connection.properties").create

    val libPath = new java.io.File("").getAbsolutePath
    Console.println(libPath)

    // Create the stage for the JAR files and upload the JAR files to the stage.
    uploadDemoFiles(jarStageName, jarFilePattern)
  }
}
