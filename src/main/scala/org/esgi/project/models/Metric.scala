package org.esgi.project.models

import play.api.libs.json.Json

case class Metric(
                   id: String,
                   timestamp: String,
                   latency: Int
                 )

object Metric {
  implicit val format = Json.format[Metric]
}
