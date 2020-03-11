package org.esgi.project.models.visits

import play.api.libs.json.Json

case class Metric(
                   id: String,
                   timestamp: String,
                   latency: Int
                 )

object Metric {
  implicit val format = Json.format[Metric]
}
