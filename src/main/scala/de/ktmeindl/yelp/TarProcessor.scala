package de.ktmeindl.yelp

import java.io.File
import java.nio.file.Files
import java.util.UUID

import de.ktmeindl.yelp.Constants._
import de.ktmeindl.yelp.DataStorage._
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

object TarProcessor {

  lazy val logger = LoggerFactory.getLogger(getClass)
  private var isTmpDir = false

  //========================================== Resolving the tar file and store in distributed storage ==============

  def untarAndStoreYelpData(props: PropertiesConfiguration, spark: SparkSession, tarFile: String): Unit = {
    val fs: FileSystem = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val dataDir = Option[String](props.getString(DATA_DIR)) match {
      case Some(s) => {
        logger.debug(s"Working in directory ${s}")
        s
      }
      case None => {
        isTmpDir = true
        val tmpDirPath = s"/tmp/${UUID.randomUUID().toString}"
        logger.debug(s"Working in tmp directory ${tmpDirPath}")
        tmpDirPath
      }
    }
    try {
      if (props.getBoolean(SHOULD_UNTAR, true)) {
        TarSerDe.untarToHdfs(spark.sparkContext, tarFile, dataDir, fs)
      }

      logger.info(s"Reading in data from ${dataDir}")
      val localInstances = getHdfsInstances(dataDir)                   // this is where the extracted data is stored
      val remoteInstances = getInstancesFromProps(props)                // this is where the data should be loaded to
      storeUntarredData(props, spark, localInstances, remoteInstances)
    } finally {
      if (isTmpDir) fs.delete(new org.apache.hadoop.fs.Path(dataDir), true)
    }
  }

  def storeUntarredData(props: PropertiesConfiguration, spark: SparkSession,
                        dataInstances: Seq[DataInstance], remoteInstances: Seq[DataInstance]) = {
    val dataInstanceMap = dataInstances.map(instance => instance.name -> instance).toMap
    // remoteInstances are all of the same type, hence the check of the first storage type suffices
    remoteInstances.head.storage match {
      case FileStorage(_)             => null       // nothing to do
      case CassandraStorage(keyspace) => storeJsonFilesInCassandra(props, spark, remoteInstances,
        dataInstanceMap, keyspace)
      case HDFSStorage(dir)           => storeJsonFilesInHdfs(spark, remoteInstances, dataInstanceMap, dir)
      case x: DataStorage             => throw new Exception(s"Unknown storage type '${x}'! Can't store the data there. ")
    }
  }

  /**
    * Reading in the extracted json files and writing them to a Cassandra table
    */
  private def storeJsonFilesInCassandra(props: PropertiesConfiguration,
                                        spark: SparkSession,
                                        remoteInstances: Seq[DataInstance],
                                        dataInstances: Map[String, DataInstance],
                                        keyspace: String) = {
    val client = CassandraClient(props)
    try {
      remoteInstances.foreach(instance => {
        val localInstance = dataInstances(instance.name)
        storeJsonFileInCassandra(
          client,
          spark,
          localInstance.storage.asInstanceOf[HDFSStorage].dir,
          localInstance.instanceName,
          keyspace,
          instance.instanceName,
          instance.key,
          instance.keyIsUnique)
      })
    } finally {
      client.close()
    }
  }

  /**
    * Reading in the extracted json file and writing it to a Cassandra table
    *
    * @param spark        SparkSession
    * @param dataDir      Directory where the json files are placed
    * @param filename     Short-name of the json file
    * @param keyspace     Cassandra keyspace for the data
    * @param table        Cassandra table name for the specific json file
    * @param key          Sequence of primary key column names
    * @param keyIsUnique  Indicates whether the primary key is unique or not. If not, a unique column is added.
    * @return             A Dataframe containing the parsed json data for further processing
    */
  private def storeJsonFileInCassandra(client: CassandraClient,
                                       spark: SparkSession,
                                       dataDir: String,
                                       filename: String,
                                       keyspace: String,
                                       table: String,
                                       key: Seq[String],
                                       keyIsUnique: Boolean = true) : DataFrame = {
    val dfIn = readJson(spark, getHdfsFile(dataDir, filename))

    val (df, primaryKey) = keyIsUnique match {
      case false => (dfIn.withColumn(COL_UUID, generateUUID()), key ++ Seq(COL_UUID))
      case true => (dfIn, key)
    }

    writeToCassandra(client, keyspace, table, df, primaryKey)
    df
  }

  /**
    * Reading in the extracted json files and writing them to a HDFS
    */
  private def storeJsonFilesInHdfs(spark: SparkSession, remoteInstances: Seq[DataInstance], localInstancesMap: Map[String, DataInstance], dir: String) = {
    remoteInstances.foreach(instance => {
      val localInstance = localInstancesMap(instance.name)
      storeJsonFileInHDFS(
        spark,
        localInstance.storage.asInstanceOf[HDFSStorage].dir,
        localInstance.instanceName,
        dir,
        instance.instanceName)
    })
  }

  /**
    * Reading in the extracted json file and writing it to a HDFS json
    * @param spark        SparkSession
    * @param dataDir      Directory where the json files are placed
    * @param filename     Short-name of the json file
    * @param hdfsDir      Directory in HDFS where to put the data
    * @param hdfsFile     Filename of Json in HDFS, within the directory
    */
  def storeJsonFileInHDFS(spark: SparkSession, dataDir: String, filename: String, hdfsDir: String, hdfsFile: String) = {
    val df = readJson(spark, getHdfsFile(dataDir, filename))
    writeJson(df, getHdfsFile(hdfsDir, hdfsFile))
  }


}
