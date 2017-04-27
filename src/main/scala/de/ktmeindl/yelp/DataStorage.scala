package de.ktmeindl.yelp

import java.io.File
import java.util.UUID

import de.ktmeindl.yelp.Constants._
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import scala.util.Try

object DataStorage {

  lazy val logger = LoggerFactory.getLogger(getClass)

  //========================================== Data Storage and Data Instances =====================================

  /**
    * Specific storage location for data, e.g. directory or schema
    */
  trait DataStorage
  case class FileStorage(dir: File) extends DataStorage
  case class HDFSStorage(dir: String) extends DataStorage
  case class CassandraStorage(keyspace: String) extends DataStorage

  /**
    * One instance of stored data,  e.g. one table, one file, etc.
    * @param name           global name of the instance for further processing, e.g. 'business' or 'user' table
    * @param storage        place, where the instances are stored
    * @param instanceName   name of the specific instance, e.g. filename or table name
    * @param key            Sequence of all column that combine to the key
    * @param keyIsUnique    Indicates whether the key is unique or not (only unique keys can be primary keys)
    */
  case class DataInstance(name: String, storage: DataStorage, instanceName: String,
                          key: Seq[String], keyIsUnique: Boolean = true)


  //========================================== Extract data storage information from properties ====================

  def getInstancesFromProps(props: PropertiesConfiguration) : Seq[DataInstance] = {
    props.getString(STORAGE_TYPE, TYPE_CASSANDRA).toLowerCase match {
      case TYPE_CASSANDRA   => getCassandraInstances(props)
      case TYPE_FILE        => getLocalInstances(new File(props.getString(DATA_DIR)))
    }
  }

  private def getCassandraInstances(props: PropertiesConfiguration) = {
    val storage = CassandraStorage(props.getString(C_KEYSPACE, C_KEYSPACE_DEFAULT))
    Seq(
      DataInstance(CHECKIN, storage, props.getString(C_CHECKIN, C_CHECKIN_DEFAULT), Seq(COL_BUSINESS_ID), false),
      DataInstance(BUSINESS, storage, props.getString(C_BUSINESS, C_BUSINESS_DEFAULT), Seq(COL_BUSINESS_ID)),
      DataInstance(REVIEW, storage, props.getString(C_REVIEW, C_REVIEW_DEFAULT), Seq(COL_REVIEW_ID)),
      DataInstance(TIP, storage, props.getString(C_TIP, C_TIP_DEFAULT), Seq(COL_USER_ID, COL_BUSINESS_ID)),
      DataInstance(USER, storage, props.getString(C_USER, C_USER_DEFAULT), Seq(COL_USER_ID))
    )
  }

  def getHdfsInstances(hdfsDir: String) = {
    val storage = HDFSStorage(hdfsDir)
    Seq(
      DataInstance(CHECKIN, storage, FILE_CHECKIN, Seq(COL_BUSINESS_ID), false),
      DataInstance(BUSINESS, storage, FILE_BUSINESS, Seq(COL_BUSINESS_ID)),
      DataInstance(REVIEW, storage, FILE_REVIEW, Seq(COL_REVIEW_ID)),
      DataInstance(TIP, storage, FILE_TIP, Seq(COL_USER_ID, COL_BUSINESS_ID)),
      DataInstance(USER, storage, FILE_USER, Seq(COL_USER_ID))
    )
  }

  def getLocalInstances(dir: File) = {
    val storage = FileStorage(dir)
    Seq(
      DataInstance(CHECKIN, storage, FILE_CHECKIN, Seq(COL_BUSINESS_ID), false),
      DataInstance(BUSINESS, storage, FILE_BUSINESS, Seq(COL_BUSINESS_ID)),
      DataInstance(REVIEW, storage, FILE_REVIEW, Seq(COL_REVIEW_ID)),
      DataInstance(TIP, storage, FILE_TIP, Seq(COL_USER_ID, COL_BUSINESS_ID)),
      DataInstance(USER, storage, FILE_USER, Seq(COL_USER_ID))
    )
  }

  //======================================== Read in yelp dataset ====================================================

  /**
    * Loads the set of yelp data in from various possible data sources
    *
    * @param spark    SparkSession
    * @param props    PropertiesConfiguration of the job, containing information about file storage and names
    * @return         A Map of (table name -> DataFrame) containing the yelp data
    */
  def loadYelpData(spark: SparkSession, props: PropertiesConfiguration): Try[Map[String, DataFrame]] = Try {
    val instances : Seq[DataInstance] = getInstancesFromProps(props)
    loadYelpData(spark, instances)
  }

  def loadYelpData(spark: SparkSession, instances: Seq[DataInstance]): Map[String, DataFrame] = {
    instances.map(instance => instance.name -> readDfFromInstance(spark, instance)).toMap
  }

  //========================================== Reading in data from different sources / writing out ====================

  /**
    * Reading in data from various storage types and locations.
    * @param spark          SparkSession
    * @param storage        Instance of DataStorage containing the data location
    * @param instanceName   Name of the file or table to load
    * @return               DataFrame containing the data
    */
  def readDfFromStorage(spark: SparkSession, storage: DataStorage, instanceName: String): DataFrame = {
    storage match {
      case FileStorage(dir) => readLocalFile(spark, dir, instanceName)
      case CassandraStorage(keyspace) => readCassandraTable(spark, keyspace, instanceName)
      case HDFSStorage(dir) => readJson(spark, getHdfsFile(dir, instanceName))
      case _ => throw new Exception(s"Unknown storage type '${storage}'! Can't read data in.")
    }
  }


  def readDfFromInstance(spark: SparkSession, instance: DataInstance): DataFrame = {
    readDfFromStorage(spark, instance.storage, instance.instanceName)
  }

  def readLocalFile(spark: SparkSession, dataDir: File, file: String): DataFrame = {
    readJson(spark, "file:///" + new File(dataDir, file).getAbsolutePath)
  }


  def readJson(spark: SparkSession, location: String): DataFrame = {
    logger.debug(s"Reading json data from ${location}")
    spark.read.json(location)
  }

  def writeJson(df: DataFrame, location: String): Unit = {
    if(hdfsFileExists(df.sparkSession, location)){
      logger.warn(s"HDFS file or directory ${location} already exists - skipping the step!")
      return
    }
    df.write.json(location)
  }

  def readCassandraTable(spark: SparkSession, keyspace: String, table: String): DataFrame = {
    spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> table, "keyspace" -> keyspace))
      .load
  }

  def writeToCassandra(client: CassandraClient, keyspace: String, table: String, df: DataFrame, primaryKey: Seq[String]): Unit = {
    if (!client.keyspaceExists(keyspace)) {
      client.createKeyspace(keyspace)
    }
    if (!client.tableExists(keyspace, table)) {
      val columns = df.schema.fields.map(field => (field.name, resolveDataType(field.dataType)))
      client.createTable(keyspace, table, columns, primaryKey)
    } else {
      logger.warn(s"Table ${keyspace}.${table} already exists - skipping the step!")
      return
    }

    logger.info(s"Storing data in cassandra table ${keyspace}.${table}")
    df.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> table, "keyspace" -> keyspace))
      .save
  }

  //========================================== Some helper functions =================================================

  def resolveDataType(dataType: DataType): String = {
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

  def getHdfsFile(dir: String, file: String) = {
    dir + "/" + file
  }

  def hdfsFileExists(spark: SparkSession, hdfsDirectory: String): Boolean = {
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    fs.exists(new org.apache.hadoop.fs.Path(hdfsDirectory))
  }
}
