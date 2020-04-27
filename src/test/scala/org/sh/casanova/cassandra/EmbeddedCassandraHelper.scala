package org.sh.casanova.cassandra

import com.datastax.oss.driver.api.core.CqlSession
import org.cassandraunit.utils.EmbeddedCassandraServerHelper

object EmbeddedCassandraHelper {

  lazy val session: CqlSession = {
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(20000L)
    EmbeddedCassandraServerHelper.getSession
  }

  def clean(): Unit = {
    EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
  }
}
