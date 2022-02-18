package io.kaizensolutions.virgil

import com.dimafeng.testcontainers.GenericContainer
import zio._

trait CassandraContainer {
  def getHost: Task[String]
  def getPort: Task[Int]
}
object CassandraContainer {
  def apply(cassType: CassandraType): UManaged[CassandraContainer] = {
    val nativePort         = 9042
    val datastaxEnterprise = "datastax/dse-server:6.8.19"
    val datastaxEnv = Map(
      "DS_LICENSE"     -> "accept",
      "JVM_EXTRA_OPTS" -> "-Dcassandra.initial_token=0 -Dcassandra.skip_wait_for_gossip_to_settle=0"
    )
    val vanilla = "cassandra:4"
    val vanillaEnv = Map(
      "CASSANDRA_ENDPOINT_SNITCH" -> "GossipingPropertyFileSnitch",
      "CASSANDRA_DC"              -> "dc1",
      "CASSANDRA_NUM_TOKENS"      -> "1",
      "CASSANDRA_START_RPC"       -> "false",
      "JVM_OPTS"                  -> "-Dcassandra.skip_wait_for_gossip_to_settle=0 -Dcassandra.auto_bootstrap=false"
    )

    val container = cassType match {
      case CassandraType.Plain =>
        GenericContainer(
          dockerImage = vanilla,
          env = vanillaEnv,
          exposedPorts = Seq(nativePort)
        )

      case CassandraType.Search =>
        GenericContainer(
          dockerImage = datastaxEnterprise,
          command = Seq("-s"),
          env = datastaxEnv,
          exposedPorts = Seq(nativePort)
        )

      case CassandraType.Graph =>
        GenericContainer(
          dockerImage = datastaxEnterprise,
          command = Seq("-g"),
          env = datastaxEnv,
          exposedPorts = Seq(nativePort)
        )

      case CassandraType.Analytics =>
        GenericContainer(
          dockerImage = datastaxEnterprise,
          command = Seq("-k"),
          env = datastaxEnv,
          exposedPorts = Seq(nativePort)
        )

      case CassandraType.Full =>
        GenericContainer(
          dockerImage = datastaxEnterprise,
          command = Seq("-s -k -g"),
          env = datastaxEnv,
          exposedPorts = Seq(nativePort)
        )
    }

    ZManaged
      .acquireRelease(ZIO.succeed(container.start()))(ZIO.succeed(container.stop()))
      .as(
        new CassandraContainer {
          override def getHost: Task[String] = Task(container.host)
          override def getPort: Task[Int]    = Task(container.mappedPort(9042))
        }
      )
  }
}

sealed trait CassandraType
object CassandraType {
  case object Plain     extends CassandraType
  case object Search    extends CassandraType
  case object Graph     extends CassandraType
  case object Analytics extends CassandraType
  case object Full      extends CassandraType
}
