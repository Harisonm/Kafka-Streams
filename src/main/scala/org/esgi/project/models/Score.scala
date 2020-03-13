package org.esgi.project.models

import play.api.libs.json.Json

case class Score(
                  _id: Int,
                  title: String,
                  score_sum : Double,
                  total :Int

                )
object Score {
  implicit val format = Json.format[Score]
}
