package io.radicalbit.nsdb.sql.parser

import io.radicalbit.nsdb.common.statement._
import org.scalatest.{Matchers, WordSpec}

import scala.util.Success

class DeleteSQLStatementSpec extends WordSpec with Matchers {

  private val parser = new SQLStatementParser

  "A parser instance" when {

    "receive a delete without a where condition" should {
      "fail" in {
        parser.parse(namespace = "registry", input = "DELETE FROM people") shouldBe 'failure
      }
    }

    "receive a delete containing a range selection" should {
      "parse it successfully" in {
        parser.parse(namespace = "registry", input = "delete FROM people WHERE timestamp IN (2,4)") should be(
          Success(
            DeleteSQLStatement(
              namespace = "registry",
              metric = "people",
              condition = Condition(RangeExpression(dimension = "timestamp", value1 = 2L, value2 = 4L))
            )))
      }

      "parse it successfully ignoring case" in {
        parser.parse(namespace = "registry", input = "delete FrOm people where timestamp in (2,4)") should be(
          Success(
            DeleteSQLStatement(
              namespace = "registry",
              metric = "people",
              condition = Condition(RangeExpression(dimension = "timestamp", value1 = 2, value2 = 4))
            )))
      }
    }

    "receive a delete containing a GTE selection" should {
      "parse it successfully" in {
        parser.parse(namespace = "registry", input = "DELETE FROM people WHERE timestamp >= 10") should be(
          Success(
            DeleteSQLStatement(
              namespace = "registry",
              metric = "people",
              condition = Condition(
                ComparisonExpression(dimension = "timestamp", comparison = GreaterOrEqualToOperator, value = 10L))
            )))
      }
    }

    "receive a delete containing a GT AND a LTE selection" should {
      "parse it successfully" in {
        parser.parse(namespace = "registry", input = "Delete FROM people WHERE timestamp > 2 AND timestamp <= 4") should be(
          Success(DeleteSQLStatement(
            namespace = "registry",
            metric = "people",
            condition = Condition(TupledLogicalExpression(
              expression1 = ComparisonExpression(dimension = "timestamp", comparison = GreaterThanOperator, value = 2L),
              operator = AndOperator,
              expression2 =
                ComparisonExpression(dimension = "timestamp", comparison = LessOrEqualToOperator, value = 4l)
            ))
          )))
      }
    }

    "receive a delete containing a GTE OR a LT selection" should {
      "parse it successfully" in {
        parser.parse(namespace = "registry", input = "DELETE FROM people WHERE NOT timestamp >= 2 OR timestamp < 4") should be(
          Success(DeleteSQLStatement(
            namespace = "registry",
            metric = "people",
            condition = Condition(UnaryLogicalExpression(
              expression = TupledLogicalExpression(
                expression1 =
                  ComparisonExpression(dimension = "timestamp", comparison = GreaterOrEqualToOperator, value = 2L),
                operator = OrOperator,
                expression2 = ComparisonExpression(dimension = "timestamp", comparison = LessThanOperator, value = 4L)
              ),
              operator = NotOperator
            ))
          )))
      }
    }

    "receive random string sequences" should {
      "fail" in {
        parser.parse(namespace = "registry", input = "fkjdskjfdlsf") shouldBe 'failure
      }
    }

  }
}
