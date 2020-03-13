package org.esgi.project.utils

import org.apache.kafka.streams.kstream.{Materialized, Windowed}
import org.apache.kafka.streams.scala.kstream.{KGroupedStream, KTable, TimeWindowedKStream}
import org.esgi.project.Main.toSerde
import org.esgi.project.models.{LikesOut, MoviesDetails, Score, Views, ViewsOut}
import play.api.libs.json.JsValue

object Helper {
  def createStoreFromGroupedStreamViewsOut(table: KGroupedStream[Int, JsValue], storeName: String): KTable[Int, ViewsOut] = {
    table.aggregate(createDefaultViewsOut)((_, view, aggView) => creatViewsOut(view, aggView)
    )(Materialized.as(storeName).withValueSerde(toSerde))
  }
  def createStoreFromGroupedStream(table: KGroupedStream[Int, JsValue], storeName: String): KTable[Int, MoviesDetails] = {
    table.aggregate(createDefaultMovieDetails)((_, view, aggView) => createMovieDetails(view, aggView)
    )(Materialized.as(storeName).withValueSerde(toSerde))
  }
  def createStoreFromWindowStream(table: TimeWindowedKStream[Int, JsValue], storeName: String): KTable[Windowed[Int], MoviesDetails] = {
    table.aggregate(createDefaultMovieDetails)((_, view, aggView) => createMovieDetails(view, aggView)
    )(Materialized.as(storeName).withValueSerde(toSerde))
  }

  def createDefaultMovieDetails: MoviesDetails = MoviesDetails(id = 0, title = "", start_only = 0, half = 0, full = 0)
  def createDefaultViewsOut: ViewsOut = ViewsOut(0,"",0L)
  //def createDefaultLikesOut: LikesOut = LikesOut(0,"",0L)
  def createDefaultScore: Score = Score(0,"",0L,0)
  def createMovieDetails(value: JsValue, agg: MoviesDetails): MoviesDetails = {
    val view = value.asOpt[Views].get
    MoviesDetails(
      id = view._id,
      title = view.title,
      if( view.view_category == "start_only")  agg.start_only + 1 else agg.start_only,
      if( view.view_category == "half")  agg.half + 1 else agg.half,
      if( view.view_category == "full")  agg.full + 1 else agg.full
    )
  }
  def creatViewsOut(value: JsValue, agg: ViewsOut): ViewsOut = {
    val viewParsed = value.asOpt[Views].get
    ViewsOut(_id = viewParsed._id,viewParsed.title,agg.views +1)
  }
  def creatSocre(value: LikesOut, agg: Score): Score = {
    //val likesParsed = value.asOpt[LikesOut].get
    Score(_id = value._id,title= value.title,score_sum = agg.score_sum + value.score , total = agg.total +1)
  }

  def createStoreFromGroupedStreamScore(table: KGroupedStream[Int, LikesOut], storeName: String): KTable[Int, Score] = {
    table.aggregate(createDefaultScore)((_, view, aggView) => creatSocre(view, aggView)
    )(Materialized.as(storeName).withValueSerde(toSerde))
  }
}
