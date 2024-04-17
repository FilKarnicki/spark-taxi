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

  implicit class LocalDateTimeBucketingOps(dt: LocalDateTime) {
    def bucketByMins(mins: Int): LocalDateTime = {
      val minutesAfterHour = (dt.getMinute / mins).floor.toInt * mins
      LocalDateTime
        .from(dt)
        .withMinute(minutesAfterHour)
        .withSecond(0)
    }
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

    def whatAreTopPickupDropoffPoints = {
      def popularPickupWithDropoffZones(dataset: Dataset[Trip]): DataFrame = {
        dataset
          .groupByKey(trip => (trip.PULocationID, trip.DOLocationID))
          .mapGroups((pickupWithDropoff, trips) => (pickupWithDropoff, trips) match {
            case ((pickup: Int, dropOff: Int), trip: Iterator[Trip]) =>
              (pickup, dropOff, trip.size)
          })
          .join(taxiZones, col("_1") === taxiZones.col("LocationID").as("pickup"))
          .withColumnRenamed("zone", "pickupZone")
          .drop("LocationID")
          .join(taxiZones, col("_2") === taxiZones.col("LocationID"))
          .withColumnRenamed("zone", "dropoffZone")
          .orderBy(col("_3").desc_nulls_last)
          .select(col("pickupZone"), col("dropoffZone"), col("_3").as("count"))
      }

      popularPickupWithDropoffZones(taxi.filter(_.trip_distance >= 30))
        .show // airport transfers?
      popularPickupWithDropoffZones(taxi.filter(_.trip_distance < 30))
        .show // affluent areas?
    }

    def howArePeoplePayingForTheRides = {
      taxi
        .groupByKey(_.RatecodeID)
        .mapGroups((k, v) => (k, v.size))
        .orderBy(col("_2").desc_nulls_last)
        .show
    }

    def whatAreTheTrendsInPaymentTypes = {
      // this time with DF
      taxi
        .toDF
        .groupBy(col("payment_type"), (to_date(col("tpep_pickup_datetime")).as("date")))
        .agg(count("*").as("total_trips"))
        // below would be correct for Dataset
        //.groupByKey(trip => (trip.RatecodeID, trip.tpep_pickup_datetime.getHour))
        //.mapGroups((k, v) => (k, v.size))
        .orderBy(col("payment_type"), col("date").desc_nulls_last)
        .show
    }

    def exploringRideshareForShortTripsCloseTogether = {
      val bucketedTrips =
        taxi
          .filter(_.passenger_count < 3)
          .groupByKey(trip =>
            (trip.PULocationID, trip.tpep_pickup_datetime.bucketByMins(5)))
          .mapGroups((pickupIdWithDateTime, trips) => {
            val tripList = trips.toList
            (pickupIdWithDateTime._1, pickupIdWithDateTime._2, tripList.length, tripList.map(_.total_amount).sum)
          })
          // Annoyingly this below doesn't compile
          //        .mapGroups[(Int, LocalDateTime, Int, Double)]{
          //          case ((pickupId: Int, pickupDateTime: LocalDateTime), trips: Iterator[Trip]) =>
          //            (pickupId, pickupDateTime, trips.size, trips.map(_.total_amount).sum)
          //        }
          .withColumnsRenamed(Map(
            "_1" -> "pickup_location_id",
            "_2" -> "pickup_time_bucket",
            "_3" -> "number_of_trips",
            "_4" -> "total_amount",
          ))
          .join(taxiZones, col("pickup_location_id") === col("LocationID"))
          .drop("LocationID", "service_zone")
          .orderBy(col("total_amount").desc_nulls_last)

      bucketedTrips.show() // ride sharing makes sense from the airports / should build rapid transport system

      // Proposal:
      // 1. 5% of taxi trips are groupable
      // 2. 30% of people will accept to be grouped
      // 3. $5 discount to those who accept
      // 4. $2 extra to those who don't
      // 5. costs to the company of grouped rides are -60% (staff, fuel, maintenance, etc)

      val percentGroupAccept = 0.3
      val discountForAccept = 5
      val penaltyForRefuse = 2
      val averageOperatorCostReduction: Double = 0.6 * taxi.select(avg(col("total_amount"))).as[Double].take(1).head

      val economicImpact = bucketedTrips
        .withColumn("groupedRides",
          col("number_of_trips") * percentGroupAccept)
        .withColumn("acceptedGroupedRidesEconomicImpact",
          col("groupedRides") * percentGroupAccept * (averageOperatorCostReduction - discountForAccept))
        .withColumn("rejectedGroupedRidesEconomicImpact",
          col("groupedRides") * (1 - percentGroupAccept) * penaltyForRefuse)
        .withColumn("totalEconomicImpact",
          col("acceptedGroupedRidesEconomicImpact") + col("rejectedGroupedRidesEconomicImpact"))

      economicImpact.show
      economicImpact.select(sum(col("totalEconomicImpact"))).show
    }

    whichBoroughsHaveTheMostPickupsOverall
    whatAreThePeakHours
    howAreTheTripsDistributedWithRegardsToLengthThreshold
    whatAreTopPickupDropoffPoints
    howArePeoplePayingForTheRides
    whatAreTheTrendsInPaymentTypes
    exploringRideshareForShortTripsCloseTogether
  }
}
