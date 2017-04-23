package de.ktmeindl.yelp

import java.io.File
import java.nio.file.Files
import java.util.UUID

import de.ktmeindl.yelp.Constants._
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

object DataStorage {

  lazy val logger = LoggerFactory.getLogger(getClass)
  private var isTmpDir = false

  def untarAndStoreInCassandra(props: PropertiesConfiguration, spark: SparkSession, tarFile: String): Unit = {
    val client = CassandraClient(props)
    val dataDir = props.getString(DATA_DIR, null) match {
      case s: String => new File(s)
      case _ => {
        isTmpDir = true
        val tmpDirPath = Files.createTempDirectory(UUID.randomUUID().toString).toFile
        logger.debug(s"Working in tmp directory ${tmpDirPath.getAbsolutePath}")
        tmpDirPath
      }
    }
    try {
      if (props.getBoolean(SHOULD_UNTAR, true)) {
        TarProcessor.untarToLocalFS(spark.sparkContext, tarFile, dataDir)
      }
      logger.info(s"Reading in data from ${dataDir.getAbsolutePath}")
      val keyspace = props.getString(C_KEYSPACE, C_KEYSPACE_DEFAULT)
      storeDataInCassandra(client, props, spark, dataDir, keyspace)
    } finally {
      if (isTmpDir) FileUtils.forceDelete(dataDir)
      client.close()
    }
  }

  private def storeDataInCassandra(client: CassandraClient, props: PropertiesConfiguration, spark: SparkSession,
                                   dataDir: File, keyspace: String) : Unit = {
    logger.info("Storing data in Cassandra")
    val checkinTable = props.getString(C_CHECKIN, C_CHECKIN_DEFAULT)
    val businessTable = props.getString(C_BUSINESS, C_BUSINESS_DEFAULT)
    val reviewTable = props.getString(C_REVIEW, C_REVIEW_DEFAULT)
    val tipTable = props.getString(C_TIP, C_TIP_DEFAULT)
    val userTable = props.getString(C_USER, C_USER_DEFAULT)

    processTable(
      client, spark, dataDir,
      CHECKIN_FILE,
      keyspace,
      checkinTable,
      Seq(COL_BUSINESS_ID),
      true)
    processTable(
      client, spark, dataDir,
      BUSINESS_FILE,
      keyspace,
      businessTable,
      Seq(COL_BUSINESS_ID))
    processTable(
      client, spark, dataDir,
      REVIEW_FILE,
      keyspace,
      reviewTable,
      Seq(COL_REVIEW_ID))
    processTable(
      client, spark, dataDir,
      TIP_FILE,
      keyspace,
      tipTable,
      Seq(COL_USER_ID, COL_BUSINESS_ID))
    processTable(
      client, spark, dataDir,
      USER_FILE,
      keyspace,
      userTable,
      Seq(COL_USER_ID))
  }

  private def resolveDataType(dataType: DataType): String = {
    dataType match {
      case _: StringType => "varchar"
      case _: IntegerType => "int"
      case _: LongType => "bigint"
      case _: DoubleType => "double"
      case arr: ArrayType => {
        val typ = resolveDataType(arr.elementType)
        s"set<${typ}>"
      }
      case d => throw new RuntimeException(s"Unsupported datatype '$d'!")
    }
  }

  val generateUUID = udf(() => UUID.randomUUID().toString)

  /**
    * Reading in the extracted json file and writing it to a Cassandra table
    * @param spark        SparkSession
    * @param dataDir      Directory where the json files are placed
    * @param filename     Short-name of the json file
    * @param keyspace     Cassandra keyspace for the data
    * @param table        Cassandra table name for the specific json file
    * @param key          Sequence of primary key column names
    * @param keyIsUnique  Indicates whether the primary key is unique or not. If not, a unique column is added.
    * @return             A Dataframe containing the parsed json data for further processing
    */
  private def processTable(client: CassandraClient,
                           spark: SparkSession,
                           dataDir: File,
                           filename: String,
                           keyspace: String,
                           table: String,
                           key: Seq[String],
                           keyIsUnique: Boolean = true) : DataFrame = {
    val dfIn = readFile(spark, dataDir, filename)

    val (df, primaryKey) = keyIsUnique match {
      case false => (dfIn.withColumn(COL_UUID, generateUUID()), key ++ Seq(COL_UUID))
      case true => (dfIn, key)
    }
    df.show(false)
    if(!client.keyspaceExists(keyspace)){
      client.createKeyspace(keyspace)
    }
    if(!client.tableExists(keyspace, table)){
      val columns = df.schema.fields.map(field => (field.name, resolveDataType(field.dataType)))
      client.createTable(keyspace, table, columns, primaryKey)
    } else {
      logger.warn(s"Table ${keyspace}.${table} already exists - skipping the step!")
      return df
    }

    logger.info(s"Storing data in cassandra table ${keyspace}.${table}")
    df.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> table, "keyspace" -> keyspace))
      .save

    df
  }

  private def readFile(spark: SparkSession, dataDir: File, file: String) = {
    spark.read.json(new File(dataDir, file).getAbsolutePath)
  }


}
