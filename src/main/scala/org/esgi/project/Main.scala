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

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._
import scala.collection.JavaConverters._

object Main extends PlayJsonSupport {
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
  val lastMinuteStoreName = s"lastMinuteMoviesStore-$randomUuid"
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

    val oneMinuteWindowedViews = groupedMovieById
      .windowedBy(TimeWindows.of(1.minute.toMillis).advanceBy(1.second.toMillis))

    val fiveMinutesWindowedViews = groupedMovieById
      .windowedBy(TimeWindows.of(5.minute.toMillis).advanceBy(1.second.toMillis))

    // Group by Stats
    val moviesTable: KTable[Int, StatsDetails] = groupedMovieById
      .aggregate(StatsDetails(0,0,0))((_, news, aggValue) => {
        news.asInstanceOf[Views].view_category match {
          case "half" =>
            StatsDetails(start_only = aggValue.start_only,half = aggValue.half + 1 , full = aggValue.full)
          case "full" =>
            StatsDetails(start_only = aggValue.start_only,half = aggValue.half, full = aggValue.full + 1)
          case "start_only" =>
            StatsDetails(start_only = aggValue.start_only + 1,half = aggValue.half, full = aggValue.full)
        }
      }
      )(Materialized.as(lastMinuteStoreName).withValueSerde(Serdes.Long))


    builder.build()
  }


  def routes(): Route = {
    concat(
      path("some" / "route" / Segment) {
        (id: String) =>
          get { context: RequestContext =>
            context.complete(
              Response(id = id, message = s"Hi, here's your id: $id")
            )
          }
      },
      path("movies") {
        get {
          complete {
            Response(id = "foo", message = "Another silly message")
          }
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
