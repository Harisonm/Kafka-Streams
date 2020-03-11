package org.esgi.project.models

import play.api.libs.json.Json

case class Scores(
                     title: String,
                     score: Long
                 )
object Scores {
    implicit val format = Json.format[Scores]
}
