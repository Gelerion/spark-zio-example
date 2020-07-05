package com.gelerion.spark.zio.example.cardinality.module

import com.gelerion.spark.zio.example.spark.spark.Spark
import com.gelerion.spark.zio.example.cardinality.module.cardinalityReader.CardinalityReader
import com.gelerion.spark.zio.example.cardinality.module.model.{Field, LoadFieldSpec, LoadRangeSpec}
import com.gelerion.spark.zio.example.cardinality.module.strategy.{AggregateStrategy, Strategies}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat_ws, when}
import zio.{Task, ZIO}

object cardinalityModule {

  trait Cardinality {
    def cardinality: Cardinality.Service
  }

  object Cardinality {
    trait Service {
      def getLoadFieldSpecs(fields: Seq[Field], loadSpec: LoadRangeSpec): Task[Seq[LoadFieldSpec]]
      def aggregateRawGrayList(grayLists: Map[Field, DataFrame], strategy: AggregateStrategy): Task[Map[Field, DataFrame]]
      def enforceCardinality(fieldToGrayList: Map[Field, DataFrame], strategy: AggregateStrategy)(baseDf: DataFrame): Task[DataFrame]
    }

    trait Live extends Cardinality {
      val cardinality: Service = new Service {

        def getLoadFieldSpecs(fields: Seq[Field], loadSpec: LoadRangeSpec): Task[Seq[LoadFieldSpec]] = {
          Task(fields.flatMap(field => loadSpec.dates.map(loadDate => LoadFieldSpec(field, loadDate, loadSpec.basePath))))
        }

        def aggregateRawGrayList(grayLists: Map[Field, DataFrame], strategy: AggregateStrategy): Task[Map[Field, DataFrame]] =
          Task({
            grayLists.map { case (field, rawGrayList) =>
              val groupBy = (field.partitionBy :+ "install_date").toArray // install_date was added in one of the previous steps

              val grayListDf = rawGrayList
                .select((groupBy :+ field.name).map(col): _*)
                .groupBy(groupBy.map(col): _*)
                .agg(strategy.aggregate(field.name))
                .select(concat_ws("|", groupBy.map(col): _*).alias(field.name + "_cardinality_key"), col(s"${field.name}_set"))

              (field, grayListDf)
            }
          })

        def enforceCardinality(fieldToGrayList: Map[Field, DataFrame], strategy: AggregateStrategy)(baseDf: DataFrame): Task[DataFrame] = {
          Task({
            baseDf.sparkSession.sparkContext.setJobDescription("Applying cardinality and saving results")

            val withCardinalityKeys = fieldToGrayList.keys.foldLeft(baseDf)((df, field) => df
              .transform { df =>
                field.name match {
                  case "eventName" =>
                    df.withColumn(field.name + "_cardinality_key", concat_ws("|", col("app_id"), col("ltv_day")))
                  case _ =>
                    df.withColumn(field.name + "_cardinality_key", concat_ws("|", col("app_id"), col("ltv_day")))

                }
              })

            val afterApplyingCardinality = fieldToGrayList.foldLeft(withCardinalityKeys) { case (df, (field, grayList)) =>
              val attrName = field.name
              df
                .join(grayList, Seq(attrName + "_cardinality_key"), "leftouter")
                .withColumn("_exceeded_limit", col(field.name).isNotNull and col(attrName + "_set").isNotNull and !strategy.lookup(field.name))
                .withColumn(attrName, when(col("_exceeded_limit"), field.replacement).otherwise(col(field.name)))
                .drop("_exceeded_limit", attrName + "_cardinality_key", attrName + "_set")
            }

            afterApplyingCardinality
          })
        }
      }
    }

    object CardinalityLive extends Live
  }

  def getLoadFieldSpecs(fields: Seq[Field], loadSpec: LoadRangeSpec): ZIO[Cardinality, Throwable, Seq[LoadFieldSpec]] =
    ZIO.accessM(_.cardinality.getLoadFieldSpecs(fields, loadSpec))

  def aggregateRawGrayList(grayLists: Map[Field, DataFrame], strategy: AggregateStrategy): ZIO[Cardinality, Throwable, Map[Field, DataFrame]] = {
    ZIO.accessM(_.cardinality.aggregateRawGrayList(grayLists, strategy))
  }

  def enforceCardinality(fieldToGrayList: Map[Field, DataFrame], strategy: AggregateStrategy)(baseDf: DataFrame): ZIO[Cardinality, Throwable, DataFrame] = {
    ZIO.accessM(_.cardinality.enforceCardinality(fieldToGrayList, strategy)(baseDf))
  }


  def applyCardinality(fields: Seq[Field], loadSpec: LoadRangeSpec, baseDf: DataFrame): ZIO[Cardinality with CardinalityReader with Spark, Throwable, DataFrame] = {
    for {
      loadsSpecs    <- getLoadFieldSpecs(fields, loadSpec)
      rawGrayLists  <- cardinalityReader.loadRawGrayLists(loadsSpecs)
      aggregated    <- aggregateRawGrayList(rawGrayLists, Strategies.hashingStrategy)
      result        <- enforceCardinality(aggregated, Strategies.hashingStrategy)(baseDf)
    } yield result
  }
}