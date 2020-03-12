package org.esgi.project

import java.time.Instant
import java.util.{Properties, UUID}

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.headers.LinkParams.title
import akka.http.scaladsl.server.Directives.{complete, concat, get, path, _}
import akka.http.scaladsl.server.{RequestContext, Route}
import akka.stream.ActorMaterializer
import org.apache.kafka.streams.kstream.{JoinWindows, Joined, Materialized, Produced, Serialized, TimeWindows, Windowed}
import org.apache.kafka.streams.state.{QueryableStoreType, QueryableStoreTypes, ReadOnlyKeyValueStore, ReadOnlyWindowStore, WindowStoreIterator}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import org.slf4j.{Logger, LoggerFactory}
import com.typesafe.config.{Config, ConfigFactory}
import de.heikoseeberger.akkahttpplayjson.PlayJsonSupport
import org.esgi.project.models.Response
import org.esgi.project.models.movies.{Likes, Movies, Stats, StatsDetails, ViewCount, Views}
import play.api.libs.json.{JsValue, Json}
import io.github.azhur.kafkaserdeplayjson.{PlayJsonSupport => azhurPlay}
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._
import scala.collection.JavaConverters._

object Main extends PlayJsonSupport with azhurPlay{
  implicit val system: ActorSystem = ActorSystem.create("this-system")
  implicit val materializer: ActorMaterializer = ActorMaterializer.create(system)
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val config: Config = ConfigFactory.load()

  val props: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-streamer-apps")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    p
  }

  // randomize store names
  val randomUuid = UUID.randomUUID.toString
  val allTimeStoreName = s"allTimeMoviesStore-$randomUuid"
  val oneMinuteStoreName = s"oneMinuteMoviesStore-$randomUuid"
  val fiveMinutesStoreName = s"fiveMinuteMoviesStore-$randomUuid"

  val streams: KafkaStreams = new KafkaStreams(buildProcessingGraph, props)
  streams.start()

  def buildProcessingGraph: Topology = {
    import Serdes._

    val builder: StreamsBuilder = new StreamsBuilder

    val likesStream: KStream[String, JsValue] = builder.stream[String, String]("likes")
      .mapValues(value => Json.parse(value))

    val viewsStream: KStream[String, JsValue] = builder.stream[String, String]("views")
      .mapValues(value => Json.parse(value))

    // repartition by URL instead of random id
    val groupedMovieById: KGroupedStream[Int, JsValue] = viewsStream
      .map { (_, view) =>
        val parsedVisit = view.as[Views]
        (parsedVisit._id, view)
      }
      .groupByKey(Serialized.`with`(Serdes.Integer, PlaySerdes.create))

    // window per asked time frames
    val allWindowedViews = groupedMovieById
    val oneMinuteWindowedViews = groupedMovieById
      .windowedBy(TimeWindows.of(1.minute.toMillis).advanceBy(1.second.toMillis))

    val fiveMinutesWindowedViews = groupedMovieById
      .windowedBy(TimeWindows.of(5.minute.toMillis).advanceBy(1.second.toMillis))

    def aggViewCategory(views: JsValue, agg:StatsDetails):StatsDetails = {
      val view = views.asOpt[Views].get
      view.view_category match {
        case "half" =>
          StatsDetails(id =view._id, start_only = agg.start_only,half = agg.half + 1 , full = agg.full)
        case "full" =>
          StatsDetails(id =view._id, start_only = agg.start_only,half = agg.half, full = agg.full + 1)
        case "start_only" =>
          StatsDetails(id =view._id, start_only = agg.start_only + 1,half = agg.half, full = agg.full)
      }
    }

    // Group by Stats
    val aggViewsAllTime: KTable[Int, StatsDetails] = allWindowedViews
      .aggregate(StatsDetails(0,0,0,0))((_, views, aggValue) => aggViewCategory(views,aggValue)
      )(Materialized.as(allTimeStoreName).withValueSerde(toSerde))

    val aggOneMinute: KTable[Windowed[Int], StatsDetails] = oneMinuteWindowedViews
      .aggregate(StatsDetails(0,0,0,0))((_, views, aggValue) => aggViewCategory(views,aggValue)
      )(Materialized.as(oneMinuteStoreName).withValueSerde(toSerde))

    val aggfiveMinute: KTable[Windowed[Int], StatsDetails] = fiveMinutesWindowedViews
      .aggregate(StatsDetails(0,0,0,0))((_, views, aggValue) => aggViewCategory(views,aggValue)
      )(Materialized.as(fiveMinutesStoreName).withValueSerde(toSerde))

    builder.build()
  }


  def routes(): Route = {
    concat(
      path("movies" / Segment) {
        (id : String) =>
            get {

              // Stats Details
              val toTime = Instant.now().toEpochMilli
              val oneMinuteTime = toTime - (60 * 1000)
              val fiveMinutesTime = toTime - (5 * 60 * 1000)

              val allTimeViewsKVStore: ReadOnlyKeyValueStore[Int, StatsDetails] = streams.store(allTimeStoreName, QueryableStoreTypes.keyValueStore[Int, StatsDetails]())
              val oneMinuteViewsKVStore: ReadOnlyWindowStore[Int, StatsDetails] = streams.store(oneMinuteStoreName, QueryableStoreTypes.windowStore[Int, StatsDetails]())
              val fiveMinutesViewKVStore: ReadOnlyWindowStore[Int, StatsDetails] = streams.store(fiveMinutesStoreName, QueryableStoreTypes.windowStore[Int, StatsDetails]())

              val allTimeView: StatsDetails = allTimeViewsKVStore.all().asScala.map(_.value).filter(_.id == id.toInt).next()
              val oneMinuteView: StatsDetails = oneMinuteViewsKVStore.fetch(id.toInt, oneMinuteTime, toTime).next().value
              val fiveMinuteView: StatsDetails = fiveMinutesViewKVStore.fetch(id.toInt, fiveMinutesTime, toTime).next().value

              def countView(details: StatsDetails): Int = details.start_only + details.half + details.full

              complete(

                Movies(
                  _id = id.toInt,
                  title = "Movie title",
                  view_count = countView(allTimeView),
                  stats = Stats(
                    past = allTimeView,
                    last_minute = oneMinuteView,
                    last_five_minutes = fiveMinuteView
                  )
                )
              )
            }
      },
      path("stats" / "ten" / "best" / Segment) {
        info : String => {
          get{
            info match{
              case "score" =>
                complete(info)
              case "views" =>
                complete(info)
            }
          }
        }
      },
      path("stats" / "ten" / "worst" / Segment) {
        info: String => {
          get {
            info match {
              case "score" =>
                complete(info)
              case "views" =>
                complete(info)
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
