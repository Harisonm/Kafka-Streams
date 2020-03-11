package org.esgi.project.models

import play.api.libs.json.Json

case class Views(
                    id: String,
                    title: String,
                    views: Int
                )
object Views {
    implicit val format = Json.format[Views]
}
