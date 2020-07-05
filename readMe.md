# Example application that leverages [ZIO](https://github.com/zio/zio) for writing fully functional code in Spark/Scala


# Code
```
def application: ZIO[Services, Throwable, DataFrame] = {
  for {
    baseDf           <- reader.parquet("abc/parquet")
    afterCardinality <- cardinalityModule.applyCardinality(fields, loadRangeSpec, baseDf)
    //transformed <- transformer.transform(afterCardinality)
    //_           <- saver.parquet(transformed)
  } yield afterCardinality
}
```
  
Inject services
```

val liveServices = new Cardinality with CardinalityReader with Spark with Reader {
  def cardinality: Cardinality.Service = CardinalityLive.cardinality
  def cardinalityReader: CardinalityReader.Service = CardinalityReaderLive.cardinalityReader
  def spark: Spark.Service = SparkLive.spark
  def reader: Reader.Service = ReaderLive.reader
}
```
Run application
```
val runtime: DefaultRuntime = new DefaultRuntime {}
runtime.unsafeRun(application.provide(liveServices))
```  
