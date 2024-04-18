package eu.karnicki

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


object ApplicationForCloud {
  def main(args: Array[String]): Unit = {
    val inputPath = args.head
    val outputDestination = args(1)
    val spark = SparkSession.builder()
      .config("spark.master", "local")
      .appName("Taxi analysis")
      .getOrCreate()
    import spark.implicits._

    val taxi = spark.read
      //.load("src/main/resources/data/yellow_taxi_jan_25_2018")
      //.load("src/main/resources/data/NYC_taxi_2009-2016.parquet")
      .load(inputPath)
    taxi.printSchema

    val taxiZones = spark.read
      .option("header", "true")
      .schema(StructType(Array(
        StructField("LocationID", IntegerType),
        StructField("Borough", StringType),
        StructField("Zone", StringType),
        StructField("service_zone", StringType)
      )))
      .csv("src/main/resources/data/taxi_zones.csv")

    val percentGroupAttempt = 0.05
    val percentGroupAccept = 0.3
    val discountForAccept = 5
    val penaltyForRefuse = 2
    val averageOperatorCostReduction: Double = 0.6 * taxi.select(avg(col("total_amount"))).as[Double].take(1).head

    val economicImpact =
      taxi
        .select(
          round(unix_timestamp(col("pickup_datetime")) / 300).cast("integer").as("fiveMinId"),
          col("pickup_taxizone_id"),
          col("total_amount"))
        .groupBy(col("pickup_taxizone_id"), col("fiveMinId"))
        .agg(
          count("*").as("total_trips"),
          sum("total_amount").as("total_amount"))
        .withColumn("time_bucket", from_unixtime(col("fiveMinId") * 300))
        .drop("fiveMinId")
        .join(taxiZones, col("pickup_taxizone_id") === col("LocationID"))
        .drop("LocationID", "service_zone")
        .orderBy(col("total_amount").desc_nulls_last)
        .withColumn("groupedRides", col("total_trips") * percentGroupAttempt)
        .withColumn("acceptedGroupedRidesEconomicImpact", col("groupedRides") * percentGroupAccept * (averageOperatorCostReduction - discountForAccept))
        .withColumn("rejectedGroupedRidesEconomicImpact", col("groupedRides") * (1 - percentGroupAccept) * penaltyForRefuse)
        .withColumn("totalImpact", col("acceptedGroupedRidesEconomicImpact") + col("rejectedGroupedRidesEconomicImpact"))
        .orderBy(col("totalImpact").desc_nulls_last)

    economicImpact.write.option("header", "true").csv(outputDestination)
    economicImpact.select(sum(col("totalImpact")).as("grand_total")).show
  }
}
