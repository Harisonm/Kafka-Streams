package org.esgi.project.models

import play.api.libs.json.Json

case class ViewsOut(
                  _id: Int,
                  title: String,
                  views: Long
                )
object ViewsOut {
  implicit val format = Json.format[ViewsOut]
}
