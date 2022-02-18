package io.kaizensolutions.virgil
import com.datastax.oss.driver.api.core.{CqlSession, CqlSessionBuilder}
import io.kaizensolutions.virgil.configuration.PageState
import io.kaizensolutions.virgil.internal.CQLExecutorImpl
import io.kaizensolutions.virgil.internal.Proofs.=:!=
import zio.stream._
import zio.{RIO, RLayer, Task, TaskManaged, ZIO, ZManaged}

trait CQLExecutor {
  def execute[A](in: CQL[A]): Stream[Throwable, A]

  def executePage[A](in: CQL[A], pageState: Option[PageState])(implicit ev: A =:!= MutationResult): Task[Paged[A]]
}
object CQLExecutor {
  def execute[A](in: CQL[A]): ZStream[CQLExecutor, Throwable, A] =
    ZStream.serviceWithStream(_.execute(in))

  def executePage[A](in: CQL[A], pageState: Option[PageState] = None)(implicit
    ev: A =:!= MutationResult
  ): RIO[CQLExecutor, Paged[A]] = ZIO.serviceWithZIO[CQLExecutor](_.executePage(in, pageState))

  val live: RLayer[CqlSessionBuilder, CQLExecutor] =
    ZManaged.serviceWithManaged[CqlSessionBuilder](apply).toLayer

  val sessionLive: RLayer[CqlSession, CQLExecutor] =
    ZManaged.serviceWith[CqlSession](fromCqlSession).toLayer

  /**
   * Create a CQL Executor from an existing Datastax Java Driver's CqlSession
   * Note that the user is responsible for the lifecycle of the underlying
   * CqlSession
   * @param session
   *   is the underlying Datastax Java Driver's CqlSession
   * @return
   *   the ZIO Cassandra Session
   */
  def fromCqlSession(session: CqlSession): CQLExecutor =
    new CQLExecutorImpl(session)

  def apply(builder: CqlSessionBuilder): TaskManaged[CQLExecutor] = {
    val acquire = Task.attempt(builder.build())
    val release = (session: CqlSession) => ZIO(session.close()).ignore

    ZManaged
      .acquireReleaseWith(acquire)(release)
      .map(new CQLExecutorImpl(_))
  }
}
