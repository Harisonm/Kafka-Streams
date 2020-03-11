package org.esgi.project.models

import play.api.libs.json.Json

case class VisitCount(
                  url: String,
                  count:Long,
                )
object VisitCount {
  implicit val format = Json.format[VisitCount]
}
