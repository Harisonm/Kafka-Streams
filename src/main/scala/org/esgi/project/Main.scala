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
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsConfig, Topology}
import org.slf4j.{Logger, LoggerFactory}
import com.typesafe.config.{Config, ConfigFactory}
import de.heikoseeberger.akkahttpplayjson.PlayJsonSupport
import org.esgi.project.models._
import play.api.libs.json.{JsValue, Json}
import io.github.azhur.kafkaserdeplayjson.{PlayJsonSupport => azhurPlay}

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._

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
  val allTimeStoreName = s"allTimeViewsStore-$randomUuid"
  val oneMinuteStoreName = s"oneMinuteViewsStore-$randomUuid"
  val fiveMinutesStoreName = s"fiveMinuteViewsStore-$randomUuid"

  // Run streams
  val streams: KafkaStreams = new KafkaStreams(buildProcessingGraph, props)
  streams.start()

  def buildProcessingGraph: Topology = {
    import Serdes._
    import scala.collection.JavaConverters._


    val builder: StreamsBuilder = new StreamsBuilder

    val likesStream: KStream[String, JsValue] = builder.stream[String, String]("likes").mapValues(value => Json.parse(value))
    val viewsStream: KStream[String, JsValue] = builder.stream[String, String]("views").mapValues(value => Json.parse(value))

    val groupedById: KGroupedStream[Int, JsValue] = viewsStream
        .map { (_, view) => (view.as[Views]._id, view) }
        .groupByKey(Serialized.`with`(Serdes.Integer, PlaySerdes.create))


    // window per asked time frames
    val allTimeViews = groupedById
    val oneMinuteWindowedViews = groupedById.windowedBy(TimeWindows.of(1.minute.toMillis).advanceBy(1.second.toMillis))
    val fiveMinutesWindowedViews = groupedById.windowedBy(TimeWindows.of(5.minute.toMillis).advanceBy(1.second.toMillis))


    def aggViewCategory(value: JsValue, agg: MoviesDetails): MoviesDetails = {
      val view = value.asOpt[Views].get
      MoviesDetails(
        id = view._id,
        title = view.title,
        if( view.view_category == "start_only")  agg.start_only + 1 else agg.start_only,
        if( view.view_category == "half")  agg.half + 1 else agg.half,
        if( view.view_category == "full")  agg.full + 1 else agg.full
      )
    }

    val allTimeViewsTable: KTable[Int, MoviesDetails] = allTimeViews
        .aggregate(MoviesDetails(0,"",0,0,0))((_, view, aggView) => aggViewCategory(view, aggView)
        )(Materialized.as(allTimeStoreName).withValueSerde(toSerde))

    val oneMinuteViewsTable: KTable[Windowed[Int], MoviesDetails] = oneMinuteWindowedViews
        .aggregate(MoviesDetails(0,"",0,0,0))((_, view, aggView) => aggViewCategory(view, aggView)
        )(Materialized.as(oneMinuteStoreName).withValueSerde(toSerde))

    val fiveMinuteViewsTable: KTable[Windowed[Int], MoviesDetails] = fiveMinutesWindowedViews
        .aggregate(MoviesDetails(0,"",0,0,0))((_, view, aggView) => aggViewCategory(view, aggView)
        )(Materialized.as(fiveMinutesStoreName).withValueSerde(toSerde))


    builder.build()
  }

  def routes(): Route = {
    import scala.collection.JavaConverters._

    concat(
      path("movies" / Segment) {
        (id: String) =>
          get {

            // Stats Details
            val toTime = Instant.now().toEpochMilli
            val oneMinuteTime = toTime - (60 * 1000)
            val fiveMinutesTime = toTime - (5 * 60 * 1000)

            val allTimeViewsKVStore: ReadOnlyKeyValueStore[Int, MoviesDetails] = streams.store(allTimeStoreName, QueryableStoreTypes.keyValueStore[Int, MoviesDetails]())
            val oneMinuteViewsKVStore: ReadOnlyWindowStore[Int, MoviesDetails] = streams.store(oneMinuteStoreName, QueryableStoreTypes.windowStore[Int, MoviesDetails]())
            val fiveMinutesViewKVStore: ReadOnlyWindowStore[Int, MoviesDetails] = streams.store(fiveMinutesStoreName, QueryableStoreTypes.windowStore[Int, MoviesDetails]())

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
      path("stats" / "ten" / "best" / Segment) {
        metric: String =>
          get { context: RequestContext =>
            context.complete(
              Response(id = metric, message = s"Hi, here's your metric: $metric")
            )
          }
      },
      path("stats" / "ten" / "worst" / Segment) {
        metric: String =>
          get { context: RequestContext =>
            context.complete(
              Response(id = metric, message = s"Hi, here's your metric: $metric")
            )
          }
      }
    )
  }

  def main(args: Array[String]) {
    Http().bindAndHandle(routes(), "0.0.0.0", 8080)
    logger.info(s"App started on 8080")
  }
}
