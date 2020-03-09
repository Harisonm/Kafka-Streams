package org.esgi.project

import java.util
import java.util.Properties

import scala.collection.JavaConverters._
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives.{complete, concat, get, path, _}
import akka.http.scaladsl.server.{RequestContext, Route}
import akka.stream.ActorMaterializer
import com.typesafe.config.{Config, ConfigFactory}
import de.heikoseeberger.akkahttpplayjson.PlayJsonSupport
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.esgi.project.models.Response
import org.apache.kafka.clients.producer._
import org.slf4j.{Logger, LoggerFactory}
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import scala.concurrent.ExecutionContextExecutor

object Main extends PlayJsonSupport {
  implicit val system: ActorSystem = ActorSystem.create("this-system")
  implicit val materializer: ActorMaterializer = ActorMaterializer.create(system)

  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val config: Config = ConfigFactory.load()

  def consumeFromKafka(topic: String): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "latest")
    props.put("group.id", "consumer-group")
    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)

    consumer.subscribe(util.Arrays.asList(topic))
    while (true) {
      val record = consumer.poll(1000).asScala
      for (data <- record.iterator)
        println(data.value())
    }
  }

  /*  def writeToKafka(topic: String,assets: Map[ String,Int]): Unit = {

    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks","all")
    val producer = new KafkaProducer[String, String](props)

    val record = {
      new ProducerRecord[String, String](topic, assets {case (key, value) => (key, value + 1)})
    }
    producer.send(record)
    producer.close()
  }*/
  def writeToKafka(topic: String): Unit = {

    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks", "all")
    val producer = new KafkaProducer[String, String](props)

    val record = {
      new ProducerRecord[String, String](topic, "key", "value")
    }
    producer.send(record)
    producer.close()
  }

  def main(args: Array[String]) {
    //var m_test = Map("Ayushi" -> 0, "Megha" -> 1)
    consumeFromKafka("visits")
    //writeToKafka("mani")
  }
}
