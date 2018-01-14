package traits

import org.apache.spark.sql.SparkSession

import scala.annotation.meta.getter

trait SparkProvider {
  @(transient@getter) lazy val spark: SparkSession =
    SparkSession.builder()
      .appName("Rationales Generator")
      .master("local[3]")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
}
