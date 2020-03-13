package org.esgi.project.models

import play.api.libs.json.Json

case class Likes(
                    _id: Int,
                    score: Double
                 )
object Likes {
    implicit val format = Json.format[Likes]
}
