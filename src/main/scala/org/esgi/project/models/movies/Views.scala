package org.esgi.project.models.movies

import play.api.libs.json.Json

case class Views(
                    _id: Int,
                    title: String,
                    view_category: String
                )
object Views {
    implicit val format = Json.format[Views]
}
