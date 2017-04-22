package de.ktmeindl.yelp

import java.net.InetSocketAddress
import java.util

import com.datastax.driver.core.Cluster
import org.apache.commons.configuration.PropertiesConfiguration
import Constants._

object CassandraClient {

  private val props = new PropertiesConfiguration()
  props.load(PROP_FILE)

  def main(args: Array[String]): Unit ={
    var cluster : Cluster = null
    try {
      val addresses = new util.ArrayList[InetSocketAddress]()
      props.getStringArray(CASSANDRA_HOSTS)
        .map(x => new InetSocketAddress(x.split(':')(0), x.split(':')(1).toInt))
        .foreach(addresses.add)


      cluster = Cluster.builder()
        .addContactPointsWithPorts(addresses)
        .build()
      val session = cluster.connect()

      val rs = session.execute("select release_version from system.local")
      val row = rs.one()
      System.out.println(row.getString("release_version"))
    } finally {
      if (cluster != null) cluster.close();
    }
  }

}
