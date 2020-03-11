package org.esgi.project.models.visits

import play.api.libs.json.{Json, OFormat}

case class VisitWithLatency(
                             id: String,
                             sourceIp: String,
                             url: String,
                             timestamp: String,
                             latency: Int
                           )

object VisitWithLatency {
  implicit val format: OFormat[VisitWithLatency] = Json.format[VisitWithLatency]
}

