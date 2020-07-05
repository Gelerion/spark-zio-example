package com.gelerion.spark.zio.example.repositroy

import com.gelerion.spark.zio.example.spark.spark.Spark
import com.gelerion.spark.zio.example.spark.spark
import org.apache.spark.sql.{DataFrame, SparkSession}
import zio.{Task, ZIO}

object reader {
  trait Reader {
    def reader: Reader.Service
  }

  object Reader {

    trait Service {
      def parquet(path: String)(spark: SparkSession): Task[DataFrame]
    }

    trait Live extends Reader {
      val reader: Service = new Service {
        def parquet(path: String)(spark: SparkSession): Task[DataFrame] = {
          Task(spark.read.parquet(path))
        }
      }
    }
  }

  object ReaderLive extends Reader.Live

  def parquet(path: String): ZIO[Reader with Spark, Throwable, DataFrame] =
    for {
      session <- spark.session()
      result  <- reader.parquet(path, session)
    } yield result


  def parquet(path: String, spark: SparkSession): ZIO[Reader, Throwable, DataFrame] =
    ZIO.accessM(_.reader.parquet(path)(spark))
}