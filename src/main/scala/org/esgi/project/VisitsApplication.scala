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
import org.esgi.project.models.visits.{Metric, Visit, VisitCount, VisitWithLatency}
import play.api.libs.json.{JsValue, Json}

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._

object VisitsApplication extends PlayJsonSupport {
  implicit val system: ActorSystem = ActorSystem.create("this-system")
  implicit val materializer: ActorMaterializer = ActorMaterializer.create(system)
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val config: Config = ConfigFactory.load()

  // Configure Kafka Streams

  val props: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-streamer-apps")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    p
  }

  // randomize store names
  val randomUuid = UUID.randomUUID.toString
  val thirtySecondsStoreName = s"thirtySecondsVisitsStore-$randomUuid"
  val oneMinuteStoreName = s"oneMinuteVisitsStore-$randomUuid"
  val fiveMinutesStoreName = s"fiveMinuteVisitsStore-$randomUuid"

  // Run streams
  val streams: KafkaStreams = new KafkaStreams(buildProcessingGraph, props)
  streams.start()

  def buildProcessingGraph: Topology = {
    import Serdes._

    val builder: StreamsBuilder = new StreamsBuilder

    val visitsStream: KStream[String, JsValue] = builder.stream[String, String]("visits")
      .mapValues(value => Json.parse(value))

    val metricsStream: KStream[String, JsValue] = builder.stream[String, String]("metrics")
      .mapValues(value => Json.parse(value))

    // repartition by URL instead of random id
    val groupedByUrl: KGroupedStream[String, JsValue] = visitsStream
      .map { (_, visit) =>
        val parsedVisit = visit.as[Visit]
        (parsedVisit.url, visit)
      }
      .groupByKey(Serialized.`with`(Serdes.String, PlaySerdes.create))

    // window per asked time frames
    val thirtySecondsWindowedVisits = groupedByUrl
      .windowedBy(TimeWindows.of(30.seconds.toMillis).advanceBy(1.second.toMillis))

    val oneMinuteWindowedVisits = groupedByUrl
      .windowedBy(TimeWindows.of(1.minute.toMillis).advanceBy(1.second.toMillis))

    val fiveMinutesWindowedVisits = groupedByUrl
      .windowedBy(TimeWindows.of(5.minute.toMillis).advanceBy(1.second.toMillis))

    // count hits
    val thirtySecondsTable: KTable[Windowed[String], Long] = thirtySecondsWindowedVisits
      .count()(Materialized.as(thirtySecondsStoreName).withValueSerde(Serdes.Long)) // easy version

    val oneMinuteTable: KTable[Windowed[String], Long] = oneMinuteWindowedVisits
      .aggregate(0L)((_, _, currentValue) => currentValue + 1)(Materialized.as(oneMinuteStoreName).withValueSerde(Serdes.Long)) // aggregate version

    val fiveMinuteTable: KTable[Windowed[String], Long] = fiveMinutesWindowedVisits
      .count()(Materialized.as(fiveMinutesStoreName).withValueSerde(Serdes.Long))

    // join metrics & visits topics and create a new object which will be written to the topic augmented-metrics (use VisitWithLatency case class)
    val augmentedMetrics: KStream[String, VisitWithLatency] = visitsStream
      .join(metricsStream)(
        (visit, metric) => {
          val parsedVisit = visit.as[Visit]
          val parsedMetric = metric.as[Metric]
          VisitWithLatency(id = parsedVisit.id, sourceIp = parsedVisit.sourceIp, url = parsedVisit.url, timestamp = parsedVisit.timestamp, latency = parsedMetric.latency)
        },
        JoinWindows.of(30.seconds.toMillis)
      )(
        Joined.`with`(
          Serdes.String,
          PlaySerdes.create,
          PlaySerdes.create
        )
      )

    // write it to Kafka
    augmentedMetrics
      .mapValues(Json.toJson(_))
      .to("augmented-metrics")(Produced.`with`(Serdes.String, PlaySerdes.create))

    builder.build()
  }

  def routes(): Route = {
    concat(
      path("visits" / Segment) {
        period: String =>
          get {
            import scala.collection.JavaConverters._
            period match {
              case "30s" =>
                val kvStore30Seconds: ReadOnlyWindowStore[String, Long] = streams.store(thirtySecondsStoreName, QueryableStoreTypes.windowStore[String,Long]())
                val availableKeys = kvStore30Seconds.all().asScala.map(_.key.key()).toList.distinct
                val toTime = Instant.now().toEpochMilli
                val fromTime = toTime - (30 * 1000)

                complete(
                  availableKeys.map { key =>
                    val row: WindowStoreIterator[Long] = kvStore30Seconds.fetch(key, fromTime, toTime)
                    VisitCount(url = key, count = row.asScala.toList.last.value)
                  }
                )

              case "1m" =>
                val kvStore1minute: ReadOnlyWindowStore[String, Long] = streams.store(oneMinuteStoreName, QueryableStoreTypes.windowStore[String,Long]())
                val availableKeys = kvStore1minute.all().asScala.map(_.key.key()).toList.distinct
                val toTime = Instant.now().toEpochMilli
                val fromTime = toTime - (60 * 1000)

                complete(
                  availableKeys.map { key =>
                    val row: WindowStoreIterator[Long] = kvStore1minute.fetch(key, fromTime, toTime)
                    VisitCount(url = key, count = row.asScala.toList.last.value)
                  }
                )
            }
          }
      },
      path("some" / "other" / "route") {
        get {
          complete {
            HttpEntity("whatever")
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
