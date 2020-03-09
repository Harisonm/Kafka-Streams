package org.esgi.project.models

import play.api.libs.json.Json

case class Visit(
  id: String,
  sourceIp:String,
  url:String,
  timestamp:String,
  )
object Visit {
  implicit val format = Json.format[Visit]
}
