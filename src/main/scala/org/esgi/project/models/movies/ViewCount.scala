package org.esgi.project.models.movies

import play.api.libs.json.Json

case class ViewCount(
                       title: String,
                       count: Int,
                     )
object ViewCount {
  implicit val format = Json.format[ViewCount]
}

