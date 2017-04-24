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
    val spark = SparkSession.builder()
      .appName("Yelp_dataset_challenge")
      .master("local[*]")
      .config("spark.cassandra.connection.host",
        props.getStringArray(CASSANDRA_HOSTS).map(_.split(':')(0)).mkString(","))
      .getOrCreate()
    logger.debug("SparkSession initialized")

    args.length match {
        case 0 => logger.warn("Missing program argument. Usage: <path to tar-file>. Skipping the tar-file-handling..")
        case _ => TarProcessor.untarAndStoreYelpData(props, spark, args(0))
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