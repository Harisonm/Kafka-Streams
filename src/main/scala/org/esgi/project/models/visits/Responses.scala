package org.esgi.project.models.visits

import play.api.libs.json.Json

case class Response(
                       id: String,
                       message: String
                   )

object Response {
    implicit val format = Json.format[Responses]
}
