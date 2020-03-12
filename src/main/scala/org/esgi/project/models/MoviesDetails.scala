package org.esgi.project.models

import play.api.libs.json.Json

case class MoviesDetails(
                           id: Int,
                           title: String,
                           start_only: Int,
                           half: Int,
                           full: Int
                       )

object MoviesDetails {
  implicit val format = Json.format[MoviesDetails]
}