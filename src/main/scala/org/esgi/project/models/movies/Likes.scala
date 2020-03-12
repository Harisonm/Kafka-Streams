package org.esgi.project.models.movies

import play.api.libs.json.Json

case class Likes(
                    _id: Int,
                    score: Long
                 )
object Likes {
    implicit val format = Json.format[Likes]
}
