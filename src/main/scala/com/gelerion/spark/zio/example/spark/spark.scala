package com.gelerion.spark.zio.example.spark

import org.apache.spark.sql.SparkSession
import zio.{Task, ZIO}

object spark {
  trait Spark {
    def spark: Spark.Service
  }

  object Spark {
    trait Service {
      def session(): Task[SparkSession]
    }

    trait Live extends Spark {
      val spark: Service = new Service {
        def session(): Task[SparkSession] = {
          Task(SparkSession.builder()
            .appName("so")
            .master("yarn")
            .getOrCreate())
        }
      }
    }
  }

  object SparkLive extends Spark.Live

  def session(): ZIO[Spark, Throwable, SparkSession] = ZIO.accessM(_.spark.session())
}