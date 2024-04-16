package eu.karnicki

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions}

import java.time.LocalDateTime

object Taxi {
  case class Trip(vendorId: Int,
                  tpep_pickup_datetime: LocalDateTime,
                  tpep_dropoff_datetime: LocalDateTime,
                  passenger_count: Int,
                  trip_distance: Double,
                  RatecodeID: Int,
                  store_and_fwd_flag: String,
                  PULocationID: Int,
                  DOLocationID: Int,
                  payment_type: Int,
                  fare_amount: Double,
                  extra: Double,
                  mta_tax: Double,
                  tip_amount: Double,
                  tolls_amount: Double,
                  improvement_surcharge: Double,
                  total_amount: Double)

  case class TaxiZone(LocationID: Int, Borough: String, Zone: String, service_zone: String)

  implicit class ForFunOps[T](dataset: Dataset[T]) {
    def withIsLong: DataFrame =
      dataset.withColumn("isLong", col("trip_distance") >= 30)

    def withHour: DataFrame =
      dataset.withColumn("hour", hour(col("tpep_pickup_datetime")))

    def long: Dataset[T] =
      dataset.filter(col("isLong") === true)

    def short: Dataset[T] =
      dataset.filter(col("isLong") === false)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .config("spark.master", "local")
      .appName("Taxi analysis")
      .getOrCreate()
    import spark.implicits._

    val taxi = spark.read
      .load("src/main/resources/data/yellow_taxi_jan_25_2018")
      .as[Trip]

    val taxiZones = spark.read
      .option("header", "true")
      .schema(StructType(Array(
        StructField("LocationID", IntegerType),
        StructField("Borough", StringType),
        StructField("Zone", StringType),
        StructField("service_zone", StringType)
      )))
      .csv("src/main/resources/data/taxi_zones.csv")
      .as[TaxiZone]

    val zonesWithCounts = taxi
      .groupByKey(_.PULocationID)
      .mapGroups((location, trips) => {
        (location, trips.size)
      })

    def whichBoroughsHaveTheMostPickupsOverall = {
      zonesWithCounts
        .joinWith(taxiZones, zonesWithCounts.col("_1") === taxiZones.col("LocationID"))
        .orderBy(zonesWithCounts.col("_2").desc_nulls_last)
        .groupByKey(_._2.Borough)
        .mapGroups((borough, b) => (borough, b.map(_._1._2).sum))
        .toDF("Borough", "total_trips")
        .orderBy(col("total_trips").desc_nulls_last)
        .show

      // Alternative
      zonesWithCounts
        .toDF("location", "count")
        .join(taxiZones, col("location") === taxiZones.col("LocationID"))
        .groupBy(col("Borough"))
        .agg(functions.sum(col("count")).as("total_trips"))
        .orderBy(col("total_trips").desc_nulls_last)
        .show
    }

    def whatAreThePeakHours = {
      taxi
        .map(trip => (trip.tpep_pickup_datetime.getHour, trip))
        .groupByKey(_._1)
        .mapGroups((k, v) => (k, v.size))
        .orderBy(col("_2").desc_nulls_last)
        .limit(4)
        .toDF()
        .show

      // Alternative
      taxi
        .withColumn("hour", hour(col("tpep_pickup_datetime")))
        .groupBy(col("hour"))
        .agg(functions.count("*").as("count"))
        .orderBy(col("count").desc_nulls_last)
        .limit(4)
        .show
    }

    def howAreTheTripsDistributedWithRegardsToLengthThreshold = {
      taxi
        .select(col("trip_distance").as("distance"))
        .select(
          functions.count("*").as("count"),
          lit(30).as("threshold"),
          mean(col("distance").as("mean")),
          stddev(col("distance").as("stddev")),
          functions.min(col("distance").as("min")),
          functions.max(col("distance").as("max")))
          .show

      taxi
        .withIsLong
        .long
        .withHour
        .groupBy(col("hour"))
        .agg(
          functions.count("*").as("count"))
        .orderBy(col("count").desc_nulls_last)
        .show

      taxi
        .withIsLong
        .short
        .withHour
        .groupBy(col("hour"))
        .agg(
          functions.count("*").as("count"))
        .orderBy(col("count").desc_nulls_last)
        .show
    }

    whichBoroughsHaveTheMostPickupsOverall
    whatAreThePeakHours
    howAreTheTripsDistributedWithRegardsToLengthThreshold
  }
}
