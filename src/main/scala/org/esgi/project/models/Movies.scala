package org.esgi.project.models

import play.api.libs.json.Json

case class Movies(
                     _id: Int,
                     title: String,
                     view_count: Int,
                     stats: Stats
                 )

case class MoviesDetails(
                            id: Int,
                            title: String,
                            start_only: Int,
                            half: Int,
                            full: Int
                        )

object Movies {implicit val format = Json.format[Movies]}
object MoviesDetails {implicit val format = Json.format[MoviesDetails]}

