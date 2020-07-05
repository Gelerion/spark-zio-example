package com.gelerion.spark.zio.example.cardinality.module

import com.gelerion.spark.zio.example.cardinality.module.model.{Field, FieldData, LoadFieldSpec}
import com.gelerion.spark.zio.example.spark.spark.Spark
import com.gelerion.spark.zio.example.spark.spark
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}
import zio.{Task, ZIO}

object cardinalityReader {

  trait CardinalityReader {
    def cardinalityReader: CardinalityReader.Service
  }

  object CardinalityReader {
    trait Service {
      def loadRawGrayLists(specs: Seq[LoadFieldSpec])(spark: SparkSession): Task[Map[Field, DataFrame]] = {
        Task {
          specs.groupBy(_.field).map { case (field, loadSpecs) =>
            val rawGrayList = loadSpecs.map(spec => loadData(spec)(spark))
              .map(withInstallDateColumn)
              .map(_.data)
              .reduce(_.union(_))

            (field, rawGrayList)
          }
        }
      }

      def withInstallDateColumn(fieldData: FieldData): FieldData = {
        val df = fieldData.data
        //constant value, df schema?
        fieldData.copy(data = df.withColumn("install_date", lit(fieldData.loadDate.asMillis())))
      }

      def loadData(spec: LoadFieldSpec)(spark: SparkSession): FieldData
    }

    trait Live extends CardinalityReader {
      val cardinalityReader: Service = new Service {
        def loadData(spec: LoadFieldSpec)(spark: SparkSession): FieldData = {
          FieldData(spec.field, spec.loadDate, spark.read.parquet(spec.fullPath))
        }
      }
    }
  }

  object CardinalityReaderLive extends CardinalityReader.Live

  def loadRawGrayLists(specs: Seq[LoadFieldSpec], spark: SparkSession): ZIO[CardinalityReader, Throwable, Map[Field, DataFrame]] =
    ZIO.accessM(_.cardinalityReader.loadRawGrayLists(specs)(spark))

  def loadRawGrayLists(specs: Seq[LoadFieldSpec]): ZIO[CardinalityReader with Spark, Throwable, Map[Field, DataFrame]] =
    for {
      session <- spark.session()
      result  <- loadRawGrayLists(specs, session)
    } yield result
}
