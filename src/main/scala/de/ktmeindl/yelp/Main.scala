package de.kmeindl.yelp

import java.io._
import java.nio.file.{Files}
import java.util.UUID

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import java.util.zip.GZIPInputStream

import org.apache.commons.io.{FileUtils, IOUtils}
import org.slf4j.LoggerFactory

object Main {

  lazy val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {

    val tarFile = args.length match {
      case 0 => "file:///" + new File("data/yelp_dataset_challenge_round9.tar").getAbsolutePath()
      case _ => args(0)
    }

    val conf = new SparkConf().setAppName("Yelp_dataset_challenge").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    logger.info("Reading in tar-file " + tarFile)
    val tmpDir = Files.createTempDirectory(UUID.randomUUID().toString()).toString
    logger.debug(s"Working in tmp directory ${tmpDir}")

    try {
      untarToLocalFS(sc, tarFile, tmpDir)




    } finally {
      FileUtils.forceDelete(new File(tmpDir))
    }


    println("done")

  }

  /**
    * Un-tar an input stream of a tar-file to an output directory in local FS using Spark for the input stream
    */
  private def untarToLocalFS(sc: SparkContext, tarFile: String, output: String) = {
    // use spark to handle various filesystems
    // however, only one input file is expected (Yelp tar-file)
    sc.binaryFiles(tarFile).collect().foreach(tarFile => {
      val is = tarFile._2.open
      val dirName = new File(tarFile._1).getName
      untar(is, new File(output, dirName))
    })
  }

  /**
    * Un-tar an input stream of a tar-file to an output directory in local FS
    */
  private def untar(is: DataInputStream, output: File) = {
    Files.createDirectory(output.toPath)
    val gzip = new GZIPInputStream(is)
    val tar = new TarArchiveInputStream(gzip)
    try {
      var entry = tar.getNextTarEntry
      while (entry != null) {
        val outputFile = new File(output, entry.getName)
        if (entry.isDirectory) {
          Files.createDirectory(outputFile.toPath)
        } else {
          val outputFileStream = new FileOutputStream(outputFile)
          IOUtils.copy(tar, outputFileStream)
          outputFileStream.close()
        }
        entry = tar.getNextTarEntry
      }
    } finally {
      tar.close()
      gzip.close()
    }
  }

}