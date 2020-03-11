package org.esgi.project

import java.time.Instant
import java.util.{Properties, UUID}

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.server.Directives.{complete, concat, get, path, _}
import akka.http.scaladsl.server.{RequestContext, Route}
import akka.stream.ActorMaterializer
import org.apache.kafka.streams.kstream.{JoinWindows, Joined, Materialized, Produced, Serialized, TimeWindows, Windowed}
import org.apache.kafka.streams.state.{QueryableStoreType, QueryableStoreTypes, ReadOnlyKeyValueStore, ReadOnlyWindowStore, WindowStoreIterator}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import org.esgi.project.models.visits.{Visit, VisitCount, VisitWithLatency}
import org.slf4j.{Logger, LoggerFactory}
import com.typesafe.config.{Config, ConfigFactory}
import de.heikoseeberger.akkahttpplayjson.PlayJsonSupport
import org.esgi.project.models.Response
import org.esgi.project.models.visits.{Metric, Visit, VisitCount, VisitWithLatency}
import play.api.libs.json.{JsValue, Json}

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._

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
  val streams: KafkaStreams = new KafkaStreams(buildProcessingGraph, props)
  streams.start()

  def buildProcessingGraph: Topology = {
    import Serdes._

    val builder: StreamsBuilder = new StreamsBuilder

    val visitsStream: KStream[String, JsValue] = builder.stream[String, String]("likes")
      .mapValues(value => Json.parse(value))

    val metricsStream: KStream[String, JsValue] = builder.stream[String, String]("views")
      .mapValues(value => Json.parse(value))


    val groupedByUrl: KGroupedStream[String, JsValue] = visitsStream
      .map { (_, visit) =>
        val parsedVisit = visit.as[Visit]
        (parsedVisit.url, visit)
      }
      .groupByKey(Serialized.`with`(Serdes.String, PlaySerdes.create))

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
