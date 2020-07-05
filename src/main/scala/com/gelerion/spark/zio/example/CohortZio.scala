package com.gelerion.spark.zio.example

import com.gelerion.spark.zio.example.cardinality.module.cardinalityModule.Cardinality
import com.gelerion.spark.zio.example.cardinality.module.cardinalityModule.Cardinality.CardinalityLive
import com.gelerion.spark.zio.example.cardinality.module.cardinalityReader.{CardinalityReader, CardinalityReaderLive}
import com.gelerion.spark.zio.example.repositroy.reader.{Reader, ReaderLive}
import com.gelerion.spark.zio.example.spark.spark.{Spark, SparkLive}
import com.gelerion.spark.zio.example.cardinality.module.cardinalityModule
import com.gelerion.spark.zio.example.cardinality.module.model.{Field, FieldData, LoadFieldSpec, LoadRangeSpec, ProcessDate}
import com.gelerion.spark.zio.example.repositroy.reader
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import zio.{DefaultRuntime, Task, ZIO}

//inspired by https://github.com/jdegoes/scalaua-2019/blob/master/src/main/scala/net/degoes/ThinkingFunctionally.scala
object CohortZio {
  val runtime: DefaultRuntime = new DefaultRuntime {}
  type Services = Cardinality with CardinalityReader with Spark with Reader

  val fields: Seq[Field] = Seq(
    Field("city", "Exceeded_City", partitionBy = Seq("app_id")),
    Field("sub1", "Exceeded_Sub1", partitionBy = Seq("app_id")))

  val loadRangeSpec: LoadRangeSpec = LoadRangeSpec("base/path", ProcessDate("2019-02-12"), timeRange = 7)

  def main(args: Array[String]): Unit = {
    //Inject services
    val liveServices = new Cardinality with CardinalityReader with Spark with Reader {
      def cardinality: Cardinality.Service = CardinalityLive.cardinality
      def cardinalityReader: CardinalityReader.Service = CardinalityReaderLive.cardinalityReader
      def spark: Spark.Service = SparkLive.spark
      def reader: Reader.Service = ReaderLive.reader
    }

    //Production:
    runtime.unsafeRun(cohort.provide(liveServices))
  }

  def cohort: ZIO[Services, Throwable, DataFrame] = {
    for {
      baseDf           <- reader.parquet("abc/parquet")
      afterCardinality <- cardinalityModule.applyCardinality(fields, loadRangeSpec, baseDf)
      //transformed <- transformer.transform(afterCardinality)
      //_           <- saver.parquet(transformed)
    } yield afterCardinality
  }
}

object Test {
  val runtime: DefaultRuntime = new DefaultRuntime {}

  def main(args: Array[String]): Unit = {

    val program: ZIO[Cardinality with CardinalityReader with Spark, Throwable, DataFrame] =
      for {
        //the same code path, different services/input
        result <- cardinalityModule.applyCardinality(CohortZio.fields, CohortZio.loadRangeSpec, input)
      } yield result

    //define test services
    val testServices = new Cardinality with CardinalityReader with Spark {
      def cardinality: Cardinality.Service = CardinalityLive.cardinality

      //custom input
      def cardinalityReader: CardinalityReader.Service = new CardinalityReader.Service {
        def loadData(spec: LoadFieldSpec)(spark: SparkSession): FieldData = spec.field.name match {
          case "city" => FieldData(spec.field, spec.loadDate, rawCityCardinalityList)
          case "sub1" => FieldData(spec.field, spec.loadDate, rawCityCardinalityListSub)
        }
      }

      //run with test session
      def spark: Spark.Service = () => Task(testSession)
    }


    val result = runtime.unsafeRun(program.provide(testServices))
    result.show()
  }

  val testSession: SparkSession = getTestSession
  import testSession.implicits._

  val dt: String = "2020-01-14"

  val input: DataFrame = Seq(
    (dt, "app_1", "city_1", "sub_1"),
    (dt, "app_1", "city_2", "sub_2"),
    (dt, "app_1", "city_3", "sub_3"),
    (dt, "app_1", "city_4", "sub_4"),
    (dt, "app_1", "city_5", "sub_5"),
    (dt, "app_1", "city_6", "sub_6"),
    (dt, "app_2", "city_1", "sub_1"),
    (dt, "app_2", "city_2", "sub_2")).toDF("ltv_day", "app_id", "city", "sub1")

  val rawCityCardinalityList: DataFrame = Seq(
    ("app_1", "city_1"),
    ("app_1", "city_2"),
    ("app_1", "city_3"))
    .toDF("app_id", "city")

  val rawCityCardinalityListSub: DataFrame = Seq(
    ("app_1", "sub_1"),
    ("app_1", "sub_2"),
    ("app_1", "sub_3"))
    .toDF("app_id", "sub1")
  // ---- end

  private def getTestSession = {
    SparkSession.builder()
      .appName("af-so")
      .master("local[*]")
      .config(new SparkConf()
        .set("spark.sql.hive.thriftServer.singleSession", "true")
        .set("hive.server2.thrift.port", "10001")
        .set("javax.jdo.option.ConnectionURL", "jdbc:derby:;databaseName=metastore_db2;create=true")
      ).getOrCreate()
  }
}
