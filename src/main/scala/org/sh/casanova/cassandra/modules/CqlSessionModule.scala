package org.sh.casanova.cassandra.modules

import java.net.InetSocketAddress
import com.datastax.oss.driver.api.core.CqlSession
import scala.jdk.CollectionConverters._

case class CassandraConfig(contactPoints: Set[InetSocketAddress], localDataCenter: String)

object CassandraConfig {
  def apply(url: String, port: Int, localDataCenter: String) {
    CassandraConfig(Set(new InetSocketAddress(url, port)), localDataCenter)
  }
}

class CqlSessionModule(config: CassandraConfig) {
  def session: CqlSession = {
    val builder = CqlSession.builder()
    builder.addContactPoints(config.contactPoints.asJava)
    builder.withLocalDatacenter(config.localDataCenter)
    builder.build()
  }
}
