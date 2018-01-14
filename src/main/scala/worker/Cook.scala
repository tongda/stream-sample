package worker

import java.io.File

import model.{Event, Idea, Profile, Recommendation}
import org.apache.hadoop.fs.FileUtil
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode}
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import traits.SparkProvider
import org.apache.spark.sql.functions._

import scala.util.Random

class Cook(val spark: SparkSession) {

  import spark.implicits._

  val events: Dataset[Event] = spark
    .read
    .schema(spark.read.parquet("data/events").schema)
    .parquet("data/events")
    .as[Event]

  def pairs: Dataset[(Idea, Profile, Seq[Event])] = ???

  def run(): DataStreamWriter[Recommendation] = {
    val profiles = spark
      .read
      .parquet("data/profiles")
      .as[Profile]

    val ideas = spark
      .readStream
      .schema(spark.read.parquet("data/ideas").schema)
      .parquet("data/ideas")
      .as[Idea]

    //    ideas
    //      .map(Tuple1(_))
    //      .crossJoin(profiles.map(Tuple1(_)))
    //      .as[(Idea, Profile)]
    //      .map {
    //        case (idea, prof) => {
    //          prof.transactions.count(_ == idea.securityId) / prof.transactions.size
    //        }
    //      }

    val keyedEvents: Dataset[(String, Seq[Event])] = events
      .groupByKey(_.accountId)
      .mapGroups {
        case (accountId, es) => Tuple2(accountId, es.toSeq)
      }
    //    keyedIdeas
    //      .joinWith(keyedEvents, keyedIdeas("value") === keyedEvents("value"))
    //      .writeStream
    //      .outputMode(OutputMode.Complete())
    //      .format("console")
    //      .start()
    val profWithEvents: Dataset[Tuple1[Profile]] = keyedEvents
      .joinWith(profiles, keyedEvents("_1") === profiles("accountId"), "rightouter")
      .map {
        case ((_, evs), prof) => Profile(prof.accountId, prof.transactions, evs)
        case (null, prof) => prof
        case _ =>
          throw new Exception("Pattern Matching failed.")
      }
      .map(Tuple1(_))
    val frame = ideas
      .map(Tuple1(_))
      .crossJoin(profWithEvents)
      .as[(Idea, Profile)]
      .map {
        case (idea, prof) =>
          println(s"${prof.accountId} - ${prof.events.length} received.")
          val relatedEvents = prof.events.filter(e => e.securityId == idea.securityId)
          val score = relatedEvents.count(e => e.`type` == "LIKE") / (relatedEvents.count(e => e.`type` == "DISLIKE") + Float.MinPositiveValue)
          Recommendation(idea.id, prof.accountId, score)
      }

    frame
      .writeStream
      .queryName("Recommendations")
      .option("checkpointLocation", "data/checkpoint")
      .outputMode(OutputMode.Append())
      .format("console")
  }
}

object Cook extends SparkProvider {

  import spark.implicits._

  def reset(): Unit = {
    Seq(
      Profile("Client-1", Seq("Stork-1", "Stork-2"), Seq()),
      Profile("Client-2", Seq("Stork-3", "Stork-4"), Seq())
    )
      .toDS()
      .write
      .mode(SaveMode.Overwrite)
      .parquet("data/profiles")

    Seq(
      Idea("Idea-1", "Stork1"),
      Idea("Idea-2", "Stork2")
    )
      .toDS()
      .write
      .mode(SaveMode.Overwrite)
      .parquet("data/ideas")

    Seq(
      Event("Idea-1", "Stork1", "LIKE", "Client-1"),
      Event("Idea-1", "Stork1", "DISLIKE", "Client-1"),
      Event("Idea-1", "Stork1", "DISLIKE", "Client-1")
    )
      .toDS()
      .write
      .mode(SaveMode.Overwrite)
      .parquet("data/events")
  }

  def main(args: Array[String]): Unit = {
    spark.sparkContext.setLogLevel("ERROR")
    reset()
    FileUtil.fullyDelete(new File("data/checkpoint"))

    val eventCount = spark
      .readStream
      .schema(spark.read.parquet("data/events").schema)
      .parquet("data/events")
      .as[Event]
      .groupByKey(_ => true)
      .agg(count("*").name("count"))
      .writeStream
      .queryName("eventCount")
      .outputMode(OutputMode.Complete())
      .format("memory")
      .start()

    var cook = new Cook(spark)
    var query = cook.run().start("data/recommendations")

    while (true) {
      val oldCount = cook.events.count()
      println(s"old count - $oldCount")
      Thread.sleep(5000)
      if (new Random().nextFloat() < 0.3) {
        Seq(
          Event("Idea-1", "Stork1", "DISLIKE", "Client-1")
        )
          .toDS()
          .write
          .mode(SaveMode.Append)
          .parquet("data/events")
        Thread.sleep(1000)
        println("new event sent")
      } else {
        println("no event sent")
      }
      val newCount = spark.sql("select count from eventCount").head().getAs[Long]("count")
      println(s"new count - $newCount")
      if (newCount > oldCount) {
        println("time up to restart")
        query.stop()
        FileUtil.fullyDelete(new File("data/checkpoint"))
        Thread.sleep(5000)
        cook = new Cook(spark)
        query = cook.run().start("data/recommendations")
      }
    }
  }
}