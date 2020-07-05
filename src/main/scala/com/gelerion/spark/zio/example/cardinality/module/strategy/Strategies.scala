package com.gelerion.spark.zio.example.cardinality.module.strategy

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.ArrayContains
import org.apache.spark.sql.functions.{col, collect_set, hash}

case class AggregateStrategy(aggregate: String => Column, lookup: String => Column)

object Strategies {
  val hashingStrategy: AggregateStrategy = AggregateStrategy(
    fieldName => collect_set(hash(col(fieldName))).as(s"${fieldName}_set"),
    fieldName => new Column(ArrayContains(col(s"${fieldName}_set").expr, hash(col(fieldName)).expr))
  )
}


