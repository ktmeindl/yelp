package de.ktmeindl.yelp

import java.io.{DataInputStream, File, FileOutputStream}
import java.nio.file.Files
import java.util.zip.GZIPInputStream

import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkContext
import org.slf4j.LoggerFactory


object TarSerDe {

  lazy val logger = LoggerFactory.getLogger(getClass)

  /**
    * Untar an input stream of a tar-file to an output directory in local FS using Spark for the input stream
    */
  def untarToLocalFS(sc: SparkContext, tarFile: String, outputDir: File) = {
    val output = outputDir.getAbsolutePath
    logger.info(s"Un-tar file ${tarFile} into ${output}")
    // use spark to handle various filesystems
    // however, only one input file is expected (Yelp tar-file)
    sc.binaryFiles(tarFile).collect().foreach(tarFile => {
      val is = tarFile._2.open
      untar(is, output)
    })
  }

  /**
    * Untar an input stream of a tar-file to an output directory in local FS
    */
  private def untar(is: DataInputStream, output: String) = {
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
