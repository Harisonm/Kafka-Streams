package org.esgi.project

import java.time.Duration
import java.util.Properties

import org.apache.kafka.streams.kstream.KGroupedStream
import org.apache.kafka.streams.scala.{StreamsBuilder, kstream}
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.esgi.project.models.Visit
import play.api.libs.json.Json

object StreamApplication extends App {

  val config: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "streamer-application")
    val bootstrapServers = if (args.length > 0) args(0) else "localhost:9092"
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    p
  }

  val builder = new StreamsBuilder
  val VisitStream: KStream[String, Option[Visit]] = builder.stream[String, String]("visits")
    .mapValues(value => Json.parse(value).asOpt[Visit]).filter((_, visit) => visit.isDefined)

  val groupByUrl: kstream.KGroupedStream[String, String] = VisitStream
  .map((_, visit) => (visit.get.url, Json.stringify(Json.toJson(visit.get))))
    .groupByKey

  val streams: KafkaStreams = new KafkaStreams(builder.build(), config)
  streams.start()

}