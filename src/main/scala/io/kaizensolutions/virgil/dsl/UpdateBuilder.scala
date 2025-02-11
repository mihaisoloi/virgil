package io.kaizensolutions.virgil.dsl

import io.kaizensolutions.virgil.{CQL, MutationResult}
import zio.{Chunk, NonEmptyChunk}

class UpdateBuilder[State <: UpdateState](
  private val table: String,
  private val assignments: Chunk[Assignment],
  private val relations: Chunk[Relation],
  private val conditions: UpdateConditions
) {
  def set[A](assignment: Assignment)(implicit
    ev: UpdateState.ColumnSet <:< State
  ): UpdateBuilder[UpdateState.ColumnSet] = {
    val _ = ev
    new UpdateBuilder(table, assignments :+ assignment, relations, conditions)
  }

  def where(relation: Relation)(implicit ev: State =:= UpdateState.ColumnSet): UpdateBuilder[UpdateState.Where] = {
    val _ = ev
    new UpdateBuilder(table, assignments, relations :+ relation, conditions)
  }

  def and(relation: Relation)(implicit ev: State =:= UpdateState.Where): UpdateBuilder[UpdateState.Where] = {
    val _ = ev
    new UpdateBuilder(table, assignments, relations :+ relation, conditions)
  }

  def ifExists(implicit ev: State =:= UpdateState.Where): UpdateBuilder[UpdateState.IfExists] = {
    val _ = ev
    new UpdateBuilder(table, assignments, relations, Conditions.IfExists)
  }

  def ifCondition(
    condition: Relation
  )(implicit ev: State =:= UpdateState.Where): UpdateBuilder[UpdateState.IfConditions] = {
    val _ = ev
    new UpdateBuilder(table, assignments, relations, addIfCondition(condition))
  }

  def andIfCondition(
    condition: Relation
  )(implicit ev: State =:= UpdateState.IfConditions): UpdateBuilder[UpdateState.IfConditions] = {
    val _ = ev
    new UpdateBuilder(table, assignments, relations, addIfCondition(condition))
  }

  def build(implicit ev: State <:< UpdateState.Where): CQL[MutationResult] = {
    val _                = ev
    val readyAssignments = NonEmptyChunk.fromChunk(assignments)
    val readyRelations   = NonEmptyChunk.fromChunk(relations)

    CQL.update(
      tableName = table,
      assignments = readyAssignments.get,
      relations = readyRelations.get,
      updateConditions = conditions
    )
  }

  private def addIfCondition(condition: Relation): UpdateConditions =
    conditions match {
      case Conditions.NoConditions             => Conditions.IfConditions(NonEmptyChunk.single(condition))
      case Conditions.IfExists                 => Conditions.IfConditions(NonEmptyChunk.single(condition))
      case Conditions.IfConditions(conditions) => Conditions.IfConditions(conditions :+ condition)
    }
}

object UpdateBuilder {
  def apply(tableName: String): UpdateBuilder[UpdateState.Empty] =
    new UpdateBuilder(
      table = tableName,
      assignments = Chunk.empty,
      relations = Chunk.empty,
      conditions = Conditions.NoConditions
    )
}

sealed trait UpdateState
object UpdateState {
  sealed trait Empty        extends UpdateState
  sealed trait ColumnSet    extends Empty
  sealed trait Where        extends ColumnSet
  sealed trait IfExists     extends Where
  sealed trait IfConditions extends Where
}
