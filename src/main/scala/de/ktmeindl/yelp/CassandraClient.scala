package de.ktmeindl.yelp

import java.net.InetSocketAddress
import java.util

import com.datastax.driver.core.{Cluster, ResultSet}
import org.apache.commons.configuration.PropertiesConfiguration
import Constants._
import org.slf4j.LoggerFactory


class CassandraClient(props: PropertiesConfiguration){

  private val logger = LoggerFactory.getLogger(getClass)

  private val addresses = new util.ArrayList[InetSocketAddress]()
  props.getStringArray(CASSANDRA_HOSTS)
    .map(x => new InetSocketAddress(x.split(':')(0), x.split(':')(1).toInt))
    .foreach(addresses.add)

  private val cluster = Cluster.builder()
    .addContactPointsWithPorts(addresses)
    .build()

  /**
    * Checks, whether the specified table in the specified keyspace already exists.
    */
  def tableExists(keyspace: String, table: String): Boolean = {
    if(keyspaceExists(keyspace)){
      val metadata = cluster.getMetadata
      val it = metadata.getKeyspace(keyspace).getTables.iterator()
      while (it.hasNext){
        val tm = it.next()
        if (tm.getName.equals(table))
          return true
      }
    }
    false
  }

  /**
    * Checks, whether the specified keyspace already exists
    */
  def keyspaceExists(keyspace: String): Boolean = {
    val metadata = cluster.getMetadata
    val it = metadata.getKeyspaces.iterator()
    while (it.hasNext){
      val tm = it.next()
      if (tm.getName.equals(keyspace))
        return true
    }
    false
  }

  /**
    * Creates a keyspace with SimpleStrategy. Number of replications can be specified. Default is 1.
    */
  def createKeyspace(keyspace: String, replication: Int = 1): Unit = {
    val cmd = s"CREATE KEYSPACE ${keyspace} " +
      s"WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : ${replication} }"
    execute(cmd)
  }

  /**
    * Creates a table. Expects as input a list of String pairs (column-name, column-type) and the primary key
    */
  def createTable(keyspace: String, table: String, columns: Seq[(String, String)], key: Seq[String]): Unit = {
    val columnsString = columns.map(x => x._1 + " " + x._2).mkString(", ")
    val keyString = key.mkString("( ", ", ", " )")
    val cmd = s"CREATE TABLE ${keyspace}.${table} ( ${columnsString}, PRIMARY KEY ${keyString} )"
    execute(cmd)
  }

  def execute(cmd: String) : ResultSet = {
    logger.debug(s"Executing Cassandra command: ${cmd}")
    val session = cluster.connect()
    session.execute(cmd)
  }

  def close(): Unit = {
    if (cluster != null) cluster.close()
  }

}


object CassandraClient extends Serializable {

  private var client: CassandraClient = _
  def apply(properties: PropertiesConfiguration): CassandraClient = {
    if (null == client)
      client = new CassandraClient(properties)
    client
  }

  def close(): Unit = {
    if (client != null) client.close()
  }
}
