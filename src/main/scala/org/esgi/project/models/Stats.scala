package org.esgi.project.models

import play.api.libs.json.Json

case class Stats(
                    past: StatsDetails,
                    last_minute: StatsDetails,
                    last_five_minutes: StatsDetails
                )
case class StatsDetails(
                           start_only: Int,
                           half: Int,
                           full: Int
                       )

object StatsDetails { implicit val format = Json.format[StatsDetails] }
object Stats { implicit val format = Json.format[Stats] }
