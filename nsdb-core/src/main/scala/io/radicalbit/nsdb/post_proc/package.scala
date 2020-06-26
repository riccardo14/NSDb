/*
 * Copyright 2018-2020 Radicalbit S.r.l.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.radicalbit.nsdb
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement.{DescOrderOperator, SelectSQLStatement}
import io.radicalbit.nsdb.common.{NSDbNumericType, NSDbType}
import io.radicalbit.nsdb.index.NumericType
import io.radicalbit.nsdb.model.Schema
import io.radicalbit.nsdb.protocol.MessageProtocol.Events.{
  ExecuteSelectStatementResponse,
  SelectStatementExecuted,
  SelectStatementFailed
}
import io.radicalbit.nsdb.statement.StatementParser._

package object post_proc {

  /**
    * Applies, if needed, ordering and limiting to a sequence of chained partial results.
    * @param chainedResults sequence of chained partial results.
    * @param statement the initial sql statement.
    * @param schema metric's schema.
    * @param aggregationType aggregation type (temporal, count, sum etc.)
    * @return the final result obtained from the manipulation of the partials.
    */
  def applyOrderingWithLimit(chainedResults: Seq[Bit],
                             statement: SelectSQLStatement,
                             schema: Schema,
                             aggregationType: Option[InternalAggregation] = None): Seq[Bit] = {
    val sortedResults = aggregationType match {
      case Some(_: InternalTemporalAggregation) =>
        val temporalSortedResults = chainedResults.sortBy(_.timestamp)
        statement.limit.map(_.value).map(v => temporalSortedResults.takeRight(v)) getOrElse temporalSortedResults
      case Some(_: InternalCountSimpleAggregation) if statement.order.exists(_.dimension == "value") =>
        implicit val ord: Ordering[Any] =
          (if (statement.order.get.isInstanceOf[DescOrderOperator]) Ordering[Long].reverse
           else Ordering[Long]).asInstanceOf[Ordering[Any]]
        val sortedResults = chainedResults.sortBy(_.value.rawValue)
        statement.limit.map(_.value).map(v => sortedResults.take(v)) getOrElse sortedResults
      case _ =>
        val sortedResults = statement.order.map { order =>
          val o = schema.fieldsMap(order.dimension).indexType.ord
          implicit val ord: Ordering[Any] =
            if (statement.order.get.isInstanceOf[DescOrderOperator]) o.reverse
            else o
          chainedResults.sortBy(_.fields(statement.order.get.dimension)._1.rawValue)
        } getOrElse chainedResults
        statement.limit.map(_.value).map(v => sortedResults.take(v)) getOrElse sortedResults
    }
    statement.limit
      .map(_.value)
      .map(v => sortedResults.take(v)) getOrElse sortedResults
  }

  /**
    * Applies, if needed, ordering and limiting to a sequence of chained partial results.
    * @param chainedResults sequence of chained partial results.
    * @param statement the initial sql statement.
    * @param schema metric's schema.
    * @param aggregationType aggregation type (temporal, count, sum etc.)
    * @return the final result obtained from the manipulation of the partials.
    */
  def limitAndOrder(chainedResults: ExecuteSelectStatementResponse,
                    statement: SelectSQLStatement,
                    schema: Schema,
                    aggregationType: Option[InternalAggregation] = None): ExecuteSelectStatementResponse =
    chainedResults match {
      case SelectStatementExecuted(statement, values) =>
        SelectStatementExecuted(statement, applyOrderingWithLimit(values, statement, schema, aggregationType))
      case e: SelectStatementFailed => e
    }

  /**
    * This is a utility method to extract dimensions or tags from a Bit sequence in a functional way without having
    * the risk to throw dangerous exceptions.
    *
    * @param values the sequence of bits holding the fields to be extracted.
    * @param field the name of the field to be extracted.
    * @param extract the function defining how to extract the field from a given bit.
    */
  def retrieveField(values: Seq[Bit], field: String, extract: Bit => Map[String, NSDbType]): Map[String, NSDbType] =
    values.headOption
      .flatMap(bit => extract(bit).get(field).map(x => Map(field -> x)))
      .getOrElse(Map.empty[String, NSDbType])

  /**
    * This is a utility method in charge to associate a dimension or a tag with the given count.
    * It extracts the field from a Bit sequence in a functional way without having the risk to throw dangerous exceptions.
    *
    * @param values the sequence of bits holding the field to be extracted.
    * @param count the value of the count to be associated with the field.
    * @param extract the function defining how to extract the field from a given bit.
    */
  def retrieveCount(values: Seq[Bit],
                    count: NSDbNumericType,
                    extract: Bit => Map[String, NSDbType]): Map[String, NSDbType] =
    values.headOption
      .flatMap(bit => extract(bit).headOption.map(x => Map(x._1 -> count)))
      .getOrElse(Map.empty[String, NSDbType])

  /**
    * This utility is used when, once aggregated, maps of the Bit should be keep equal
    * @param bits the sequence of bits holding the map to fold.
    * @param extract the function defining how to extract the field from a given bit.
    */
  def foldMapOfBit(bits: Seq[Bit], extract: Bit => Map[String, NSDbType]): Map[String, NSDbType] =
    bits.map(bit => extract(bit)).fold(Map.empty[String, NSDbType])(_ ++ _)

  def postProcessingTemporalQueryResult(schema: Schema,
                                        statement: SelectSQLStatement,
                                        aggr: InternalTemporalAggregation): Seq[Bit] => Seq[Bit] = { res =>
    val numericType = schema.value.indexType.asInstanceOf[NumericType[_]]
    val chainedResult =
      res
        .groupBy(_.timestamp)
        .mapValues { bits =>
          val timestamp  = bits.head.timestamp
          val dimensions = foldMapOfBit(bits, bit => bit.dimensions)
          val tags       = foldMapOfBit(bits, bit => bit.tags)
          aggr match {
            case InternalCountTemporalAggregation =>
              Bit(timestamp, bits.map(_.value).sum, dimensions, tags)
            case InternalSumTemporalAggregation =>
              Bit(timestamp, bits.map(_.value).sum, dimensions, tags)
            case InternalMaxTemporalAggregation =>
              Bit(timestamp, bits.map(_.value).max, dimensions, tags)
            case InternalMinTemporalAggregation =>
              val nonZeroValues =
                bits.collect {
                  case x if x.value != numericType.zero =>
                    x.value
                }
              Bit(
                timestamp,
                if (nonZeroValues.isEmpty) numericType.zero else nonZeroValues.min,
                dimensions,
                tags
              )
            case InternalAvgTemporalAggregation =>
              // TODO: to implement
              throw new RuntimeException("Not implemented yet.")
          }
        }
        .values
        .toSeq

    applyOrderingWithLimit(chainedResult, statement, schema, Some(aggr))
  }

  def internalAggregationProcessing(bits: Seq[Bit], aggregationType: InternalSimpleAggregationType): Bit =
    aggregationType match {
      case _: InternalCountSimpleAggregation =>
        Bit(0, bits.map(_.value).sum, foldMapOfBit(bits, bit => bit.dimensions), foldMapOfBit(bits, bit => bit.tags))
      case _: InternalMaxSimpleAggregation =>
        Bit(0, bits.map(_.value).max, foldMapOfBit(bits, bit => bit.dimensions), foldMapOfBit(bits, bit => bit.tags))
      case _: InternalMinSimpleAggregation =>
        Bit(0, bits.map(_.value).min, foldMapOfBit(bits, bit => bit.dimensions), foldMapOfBit(bits, bit => bit.tags))
      case _: InternalSumSimpleAggregation =>
        Bit(0, bits.map(_.value).sum, foldMapOfBit(bits, bit => bit.dimensions), foldMapOfBit(bits, bit => bit.tags))
      case _: InternalFirstSimpleAggregation => bits.minBy(_.timestamp)
      case _: InternalLastSimpleAggregation  => bits.maxBy(_.timestamp)
      case _ @InternalAvgSimpleAggregation(groupField, _) =>
        val sum   = bits.flatMap(_.tags.get("sum")).map(_.asInstanceOf[NSDbNumericType]).sum
        val count = bits.flatMap(_.tags.get("count")).map(_.asInstanceOf[NSDbNumericType]).sum
        val avg   = sum / count
        Bit(
          timestamp = 0,
          value = avg,
          dimensions = Map.empty[String, NSDbType],
          tags = retrieveField(bits, groupField, bit => bit.tags)
        )
    }

}
