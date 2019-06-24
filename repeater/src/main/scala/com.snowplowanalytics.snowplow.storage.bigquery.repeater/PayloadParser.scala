package com.snowplowanalytics.snowplow.storage.bigquery.repeater

import cats.data.NonEmptyList
import cats.syntax.all._
import io.circe.{Json, JsonObject}
import io.circe.syntax._
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import BadRow.{ParsingError, ParsingErrorInfo}

object PayloadParser {
  // TODO Enes: Properly implement it
  def parse(payload: JsonObject): Either[ParsingError, Event] = {
    ParsingError(Json.obj("test" := "test".asJson), NonEmptyList.of(new ParsingErrorInfo {})).asLeft
  }
}
