package de.ktmeindl.yelp

import java.io._

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import Constants._
import org.apache.commons.configuration.PropertiesConfiguration

import scala.util.{Failure, Success}


object Main {

  lazy val logger = LoggerFactory.getLogger(getClass)
  private val props = new PropertiesConfiguration()
  props.load(PROP_FILE)


  def main(args: Array[String]): Unit = {
    logger.debug("Initializing SparkSession")

    val sparkBuilder = SparkSession.builder()
      .appName("Yelp_dataset_challenge")
//      .master("local[*]")

    // setting necessary cassandra connection details
    if(props.getString(STORAGE_TYPE).equals(TYPE_CASSANDRA)){
      sparkBuilder.config("spark.cassandra.connection.host",
        props.getStringArray(CASSANDRA_HOSTS).map(_.split(':')(0)).mkString(","))
    }
    val spark = sparkBuilder.getOrCreate()
    logger.debug("SparkSession initialized")

    args.length match {
        case 0 => logger.warn("Missing program argument. Usage: <path to tar-file>. Skipping the tar-file-handling..")
        case _ => {
          val tarFile = args(0)
          if(tarFile.startsWith("s3")){
            val conf = spark.sparkContext.hadoopConfiguration
            conf.set("fs.s3a.impl",               "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
            conf.set("fs.s3a.awsAccessKeyId",     props.getString(S3_ID, System.getProperty(S3_ID)))
            conf.set("fs.s3a.awsSecretAccessKey", props.getString(S3_KEY, System.getProperty(S3_KEY)))
            conf.set("fs.s3a.endpoint",           props.getString(S3_ENDPOINT, System.getProperty(S3_ENDPOINT)))
          }
          TarProcessor.untarAndStoreYelpData(props, spark, tarFile)
        }
      }

    DataStorage.loadYelpData(spark, props) match {
      case Failure(ex) => {
        logger.error("Failed to read yelp data!", ex)
        throw ex
      }
      case Success(tables) => {
        val userDF = tables(USER)
        val businessDF = tables(BUSINESS)
        userDF.printSchema()
        println(s"Users contained: ${userDF.count}")
      }
    }
  }

}