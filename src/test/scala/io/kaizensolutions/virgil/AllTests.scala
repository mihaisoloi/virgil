package io.kaizensolutions.virgil

import com.datastax.oss.driver.api.core.CqlSession
import io.kaizensolutions.virgil.cql._
import zio._
import zio.stream.ZStream
import zio.test._

import java.net.InetSocketAddress

object AllTests extends DefaultRunnableSpec {
  val dependencies: ZLayer[Any, Nothing, CQLExecutor] = {
    val managedSession =
      for {
        c           <- CassandraContainer(CassandraType.Plain)
        details     <- (c.getHost).zip(c.getPort).toManaged
        (host, port) = details
        session <- CQLExecutor(
                     CqlSession
                       .builder()
                       .withLocalDatacenter("dc1")
                       .addContactPoint(InetSocketAddress.createUnresolved(host, port))
                   )
        createKeyspace =
          cql"""CREATE KEYSPACE IF NOT EXISTS virgil
          WITH REPLICATION = {
            'class': 'SimpleStrategy',
            'replication_factor': 1
          }""".mutation
        useKeyspace = cql"USE virgil".mutation
        _          <- session.execute(createKeyspace).runDrain.toManaged
        _          <- session.execute(useKeyspace).runDrain.toManaged
        _          <- runMigration(session, "migrations.cql").toManaged
      } yield session

    managedSession.toLayer.orDie
  }

  def runMigration(executor: CQLExecutor, fileName: String): ZIO[Any, Throwable, Unit] = {
    val migrationCql =
      ZStream
        .fromZIO(ZIO.attemptBlocking(scala.io.Source.fromResource(fileName).getLines()))
        .flatMap(ZStream.fromIterator(_))
        .map(_.strip())
        .filterNot { l =>
          l.isEmpty ||
          l.startsWith("--") ||
          l.startsWith("//")
        }
        .runFold("")(_ ++ _)
        .map(_.split(";"))

    for {
      migrations <- migrationCql
      _          <- ZIO.foreachDiscard(migrations)(str => executor.execute(str.asCql.mutation).runDrain)
    } yield ()
  }

  override def spec: ZSpec[TestEnvironment, Any] =
    suite("Virgil Test Suite") {
      (CQLExecutorSpec.sessionSpec + UserDefinedTypesSpec.userDefinedTypesSpec) @@ TestAspect.parallel
    }.provideCustomLayerShared(dependencies)
}
