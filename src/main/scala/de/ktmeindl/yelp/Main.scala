package de.ktmeindl.yelp

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import Constants._
import org.apache.commons.configuration.PropertiesConfiguration

import scala.util.{Failure, Success}

import org.apache.spark.sql.functions._


object Main {

  lazy val logger = LoggerFactory.getLogger(getClass)
  private val props = new PropertiesConfiguration()
  // set start argument -Dyelp.properties=<path to properties file>
  props.load(System.getProperty(PROP_FILE, PROP_FILE))


  def main(args: Array[String]): Unit = {
    logger.debug("Initializing SparkSession")

    val sparkBuilder = SparkSession.builder()

    // enable local mode for faster debugging
    if(props.getString(STORAGE_TYPE, TYPE_CASSANDRA).equals(TYPE_FILE)) {
      sparkBuilder
        .master("local[*]")
        .appName("test")
        .config("spark.sql.shuffle.partitions", "10")
    }

    // setting necessary cassandra connection details
    if(props.getString(STORAGE_TYPE, TYPE_CASSANDRA).equals(TYPE_CASSANDRA)){
      sparkBuilder.config("spark.cassandra.connection.host",
        props.getStringArray(CASSANDRA_HOSTS).map(_.split(':')(0)).mkString(","))
    }

    val spark = sparkBuilder.getOrCreate()
    logger.debug("SparkSession initialized")

    // local mode for faster debugging
    if(!props.getString(STORAGE_TYPE, TYPE_CASSANDRA).equals(TYPE_FILE)){
      val shouldUntar = System.getProperty(SHOULD_UNTAR, s"${props.getBoolean(SHOULD_UNTAR, true)}").toBoolean
      if(shouldUntar){
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
            TarProcessor.untarAndStoreYelpData(props, spark, tarFile, true)
          }
        }
      } else {
        logger.info("Untar of files is not required due to configuration.")
        TarProcessor.storeYelpData(props, spark)
      }
    }


    // now analysing the data with some basic joins and selects
    DataStorage.loadYelpData(spark, props) match {
      case Failure(ex) => {
        logger.error("Failed to read yelp data!", ex)
        throw ex
      }
      case Success(tables) => {
        val userDF = tables(USER)
        val businessDF = tables(BUSINESS)
        val tipDF = tables(TIP)
        val checkinDF = tables(CHECKIN)
        val reviewDF = tables(REVIEW)

        println("\n\nStarting the analysis of the yelp dataset.\n")
        println(s"The dataset contains businesses from ${businessDF.select(col(COL_CITY)).distinct.count} cities. " +
          s"They add up from the following (showing top 10):")
        val col_business_per_city = "nr_businesses"
        val dfNrBusinesses = businessDF
          .groupBy(COL_CITY)
          .agg(count(COL_BUSINESS_ID).as(col_business_per_city))
          .sort(col(col_business_per_city).desc)
        dfNrBusinesses.show(10, false)

        val maxValue = dfNrBusinesses.groupBy().agg(max(col_business_per_city).as("max")).head.getAs[Long]("max")
        val cityMost =
          dfNrBusinesses
          .where(col(col_business_per_city) === lit(maxValue))
          .head.getAs[String](COL_CITY)

        println(s"The city with the most businesses is ${cityMost}.\n")

        println(s"The following users enjoyed their stays in ${cityMost} most based on the given ratings (showing top 10 ratings for users with more than 5 given reviews in the city):")
        val dfUsersBusinesses = businessDF.where(col(COL_CITY).as("a") === lit(cityMost))
          .join(reviewDF.as("b"), Seq(COL_BUSINESS_ID))
          .join(userDF.as("c"), Seq(COL_USER_ID))
          .select(col(s"b.${COL_STARS}"), col(s"b.${COL_USER_ID}"), col(s"c.${COL_NAME}"))

        val col_avg_stars = "avg_rating"
        val col_count_ratings = "count_ratings"
        dfUsersBusinesses
          .groupBy(COL_USER_ID, COL_NAME)
          .agg(avg(COL_STARS).as(col_avg_stars), count(COL_STARS).as(col_count_ratings))
          .where(col(col_count_ratings) > lit(5))
          .sort(col(col_avg_stars).desc)
          .show(10, false)
      }
    }
  }

}