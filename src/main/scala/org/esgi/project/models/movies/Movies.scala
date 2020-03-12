package org.esgi.project.models

import play.api.libs.json.Json

case class StatsDetails(
                           start_only: Int,
                           half: Int,
                           full: Int
                       )
object StatsDetails {
  implicit val format = Json.format[StatsDetails]
}

case class Stats(
                    past: StatsDetails,
                    last_minute: StatsDetails,
                    last_five_minutes: StatsDetails
                )
object Stats {
    implicit val format = Json.format[Stats]
}

case class Movies(
                     _id: Int,
                     title: String,
                     view_count: Int,
                     stats: Stats
                 )
object Movies {
    implicit val format = Json.format[Movies]
}

