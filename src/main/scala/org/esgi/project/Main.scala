package org.esgi.project

import java.time.Instant
import java.util.{Properties, UUID}
import math.round
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives.{complete, concat, get, path, _}
import akka.http.scaladsl.server.{RequestContext, Route}
import akka.stream.ActorMaterializer
import com.typesafe.config.{Config, ConfigFactory}
import de.heikoseeberger.akkahttpplayjson.PlayJsonSupport
import io.github.azhur.kafkaserdeplayjson.{PlayJsonSupport => azhurPlay}
import org.apache.kafka.streams.kstream.{JoinWindows, Joined, Materialized, Serialized, TimeWindows, Windowed}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.state.{QueryableStoreTypes, ReadOnlyKeyValueStore, ReadOnlyWindowStore}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import org.esgi.project.models._
import org.esgi.project.utils.PlaySerdes
import org.slf4j.{Logger, LoggerFactory}
import org.esgi.project.utils.Helper
import play.api.libs.json.{JsValue, Json}

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._
import scala.collection.JavaConverters._
object Main extends PlayJsonSupport with azhurPlay {

  implicit val system: ActorSystem = ActorSystem.create("this-system")
  implicit val materializer: ActorMaterializer = ActorMaterializer.create(system)
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val config: Config = ConfigFactory.load()

  val props: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "kazaa_movies")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    p
  }

  // randomize store names
  val randomUuid = UUID.randomUUID.toString
  val ViewsStoreName = s"ViewsStore-$randomUuid"
  val LikesStoreName = s"LikesStore-$randomUuid"
  val allTimeStoreName = s"allTimeViewsStore-$randomUuid"
  val oneMinuteStoreName = s"oneMinuteViewsStore-$randomUuid"
  val fiveMinutesStoreName = s"fiveMinuteViewsStore-$randomUuid"

  // Run streams
  val streams: KafkaStreams = new KafkaStreams(buildProcessingGraph, props)
  streams.start()

  def buildProcessingGraph: Topology = {
    import Serdes._
    import Helper._

    val builder: StreamsBuilder = new StreamsBuilder

    val likesStream: KStream[String, JsValue] = builder.stream[String, String]("likes").mapValues(value => Json.parse(value))
    val viewsStream: KStream[String, JsValue] = builder.stream[String, String]("views").mapValues(value => Json.parse(value))
    val likesOutStream: KStream[String, LikesOut] = viewsStream.join(likesStream)((views, likes) => {
      val parsedViews = views.as[Views]
      val parsedLikes = likes.as[Likes]
      LikesOut(_id = parsedViews._id,title = parsedViews.title,score = parsedLikes.score)
    },
      JoinWindows.of(30.seconds.toMillis)
    )(
      Joined.`with`(
        Serdes.String,
        PlaySerdes.create,
        PlaySerdes.create
      ))

    val groupedById: KGroupedStream[Int, JsValue] = viewsStream
        .map { (_, view) => (view.as[Views]._id, view) }
        .groupByKey(Serialized.`with`(Serdes.Integer, PlaySerdes.create))

    val groupedByIdScore: KGroupedStream[Int, LikesOut] = likesOutStream.groupBy((s,l)=> l._id)

    val allTimeViews = groupedById
    val allTimeLikes = groupedByIdScore
    val oneMinuteWindowedViews = groupedById.windowedBy(TimeWindows.of(1.minute.toMillis).advanceBy(1.second.toMillis))
    val fiveMinutesWindowedViews = groupedById.windowedBy(TimeWindows.of(5.minute.toMillis).advanceBy(1.second.toMillis))

    createStoreFromGroupedStreamScore(allTimeLikes,LikesStoreName)
    createStoreFromGroupedStreamViewsOut(allTimeViews, ViewsStoreName)
    createStoreFromGroupedStream(allTimeViews, allTimeStoreName)
    createStoreFromWindowStream(oneMinuteWindowedViews, oneMinuteStoreName)
    createStoreFromWindowStream(fiveMinutesWindowedViews, fiveMinutesStoreName)

    builder.build()
  }

  def routes(): Route = {


    concat(
      path("movies" / Segment) {
        (id: String) =>
          get {

            // Stats Details
            val allTimeViewsKVStore: ReadOnlyKeyValueStore[Int, MoviesDetails] = streams.store(allTimeStoreName, QueryableStoreTypes.keyValueStore[Int, MoviesDetails]())
            val oneMinuteViewsKVStore: ReadOnlyWindowStore[Int, MoviesDetails] = streams.store(oneMinuteStoreName, QueryableStoreTypes.windowStore[Int, MoviesDetails]())
            val fiveMinutesViewKVStore: ReadOnlyWindowStore[Int, MoviesDetails] = streams.store(fiveMinutesStoreName, QueryableStoreTypes.windowStore[Int, MoviesDetails]())

            val toTime = Instant.now().toEpochMilli
            val oneMinuteTime = toTime - (60 * 1000)
            val fiveMinutesTime = toTime - (5 * 60 * 1000)

            val allTimeView: MoviesDetails = allTimeViewsKVStore.all().asScala.map(_.value).filter(_.id == id.toInt).next()
            val oneMinuteView: MoviesDetails = oneMinuteViewsKVStore.fetch(id.toInt, oneMinuteTime, toTime).next().value
            val fiveMinuteView: MoviesDetails = fiveMinutesViewKVStore.fetch(id.toInt, fiveMinutesTime, toTime).next().value

            def countView(details: MoviesDetails): Int = details.start_only + details.half + details.full
            def getStatsDetails(moviesDetails: MoviesDetails): StatsDetails = StatsDetails(
              start_only = moviesDetails.start_only,
              half = moviesDetails.half,
              full = moviesDetails.full
            )

            complete(

              Movies(
                _id = id.toInt,
                title = allTimeView.title,
                view_count = countView(allTimeView),
                stats = Stats(
                  past = getStatsDetails(allTimeView),
                  last_minute = getStatsDetails(oneMinuteView),
                  last_five_minutes = getStatsDetails(fiveMinuteView)
                )
              )
            )
          }
      },
      path("stats"/"ten"/"best"/ Segment) {
        info : String => {
          get{
            info match {
              case "score" =>
                val LikesKVStore: ReadOnlyKeyValueStore[Int, Score] = streams.store(LikesStoreName, QueryableStoreTypes.keyValueStore[Int, Score]())
                val availableKeys = LikesKVStore.all().asScala.map(_.key).toList.distinct

                complete(
                  availableKeys.map { key =>
                    val row: Score = LikesKVStore.get(key)
                    val sum : Double = row.score_sum/row.total

                    LikesOut(_id = key,title = row.title,score = sum)
                  }.sortBy(_.score).reverse.take(10)
                )

              case "views" =>
                val ViewsKVStore: ReadOnlyKeyValueStore[Int, ViewsOut] = streams.store(ViewsStoreName, QueryableStoreTypes.keyValueStore[Int, ViewsOut]())
                val availableKeys = ViewsKVStore.all().asScala.map(_.key).toList.distinct

                complete(
                  availableKeys.map { key =>
                    val row: ViewsOut = ViewsKVStore.get(key)
                    ViewsOut(_id = key,title = row.title,views = row.views)
                  }.sortBy(_.views).reverse.take(10)
                )

            }
          }
        }
      },
      path("stats"/"ten"/"worst"/ Segment) {
        info : String => {
          get{
            info match {
              case "score" =>
                val LikesKVStore: ReadOnlyKeyValueStore[Int, Score] = streams.store(LikesStoreName, QueryableStoreTypes.keyValueStore[Int, Score]())
                val availableKeys = LikesKVStore.all().asScala.map(_.key).toList.distinct

                complete(
                  availableKeys.map { key =>
                    val row: Score = LikesKVStore.get(key)
                    val sum : Double = row.score_sum/row.total

                    LikesOut(_id = key,title = row.title,score = sum)
                  }.sortBy(_.score).take(10)
                )

              case "views" =>
                val ViewsKVStore: ReadOnlyKeyValueStore[Int, ViewsOut] = streams.store(ViewsStoreName, QueryableStoreTypes.keyValueStore[Int, ViewsOut]())
                val availableKeys = ViewsKVStore.all().asScala.map(_.key).toList.distinct

                complete(
                  availableKeys.map { key =>
                    val row: ViewsOut = ViewsKVStore.get(key)
                    ViewsOut(_id = key,title = row.title,views = row.views)
                  }.sortBy(_.views).take(10)
                )
            }
          }
        }
      }
    )
}

  def main(args: Array[String]) {
    Http().bindAndHandle(routes(), "0.0.0.0", 8080)
    logger.info(s"App started on 8080")
  }
}
