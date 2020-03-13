package org.esgi.project.models

import play.api.libs.json.Json

case class LikesOut(
                  _id: Int,
                  title: String,
                  score: Double
                )
object LikesOut {
  implicit val format = Json.format[LikesOut]
}
