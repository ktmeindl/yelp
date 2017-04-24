package de.ktmeindl.yelp

import java.io._

import org.apache.spark.sql.{SparkSession}
import org.slf4j.LoggerFactory
import Constants._
import org.apache.commons.configuration.PropertiesConfiguration


object Main {

  lazy val logger = LoggerFactory.getLogger(getClass)
  private val props = new PropertiesConfiguration()
  props.load(PROP_FILE)


  def main(args: Array[String]): Unit = {
      val tarFile = args.length match {
        case 0 => throw new Exception("Missing program argument. Usage: <path to tar-file>")
        case _ => args(0)
      }

      logger.debug("Initializing SparkSession")
      val spark = SparkSession.builder()
        .appName("Yelp_dataset_challenge")
        .master("local[*]")
        .config("spark.cassandra.connection.host",
          props.getStringArray(CASSANDRA_HOSTS).map(_.split(':')(0)).mkString(","))
        .getOrCreate()
      logger.debug("SparkSession initialized")

      TarProcessor.untarAndStoreYelpData(props, spark, tarFile)

  }



}