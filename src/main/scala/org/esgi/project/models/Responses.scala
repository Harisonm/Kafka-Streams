package org.esgi.project.models.visits

import play.api.libs.json.Json

case class Responses(
                       id: String,
                       message: String
                   )

object Responses {
    implicit val format = Json.format[Responses]
}
