package com.snowplowanalytics.snowplow.storage.bigquery.repeater

import java.time.Instant
import java.time.format.DateTimeParseException

import cats.data.NonEmptyList
import cats.implicits._
import io.circe._
import io.circe.syntax._
import io.circe.generic.semiauto._
import com.snowplowanalytics.iglu.core.circe.CirceIgluCodecs._
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent.{Contexts, UnstructEvent}
import BadRow.{ParsingError, ParsingErrorInfo}

/**
  * Parser which is used for reconverting failedInsert payload into
  * enriched event. Due to some reasons, payload can not be reconverted
  * into event fully however it can be converted mostly. In order to
  * get more information, look at the comments of "ReconstructedEvent"
  */
object PayloadParser {
  def parse(payload: JsonObject): Either[ParsingError, ReconstructedEvent] = {
    // TODO Enes: Put decoders to Scala Analytics SDK
    implicit val instantDecoder: Decoder[Instant] = Decoder.instance { c =>
      c.as[String].flatMap { str =>
        Either
          .catchOnly[DateTimeParseException](Instant.parse(str))
          .leftMap(e => DecodingFailure(e.getMessage, c.history))
      }
    }
    implicit val contextsDecoder: Decoder[Contexts] = deriveDecoder[Contexts]
    implicit val unstructEventDecoder: Decoder[UnstructEvent] = deriveDecoder[UnstructEvent]
    implicit val eventDecoder: Decoder[Event] = deriveDecoder[Event]

    val jsonsWithFlattenSchemaKey = getJsonsWithFlattenSchema(payload)

    val emptyContext = Json.obj("data" := Json.arr())
    val modified = payload
      .add("contexts", emptyContext)
      .add("derived_contexts", emptyContext)
      .add("unstruct_event", Json.obj())

    modified.asJson.as[Event] match {
      case Left(e) =>
        Left(
          ParsingError(
            payload.asJson,
            NonEmptyList.of(ParsingErrorInfo(e.message, e.history.foldLeft(List[String]())((l, i) => i.toString::l)))
          )
        )
      case Right(e) =>
        Right(ReconstructedEvent(e, jsonsWithFlattenSchemaKey))
    }

  }

  private def getJsonsWithFlattenSchema(payload: JsonObject): List[JsonWithFlattenSchemaKey] = {
    val jsonPrefixes = List("contexts", "derived_contexts", "unstruct_event")
    jsonPrefixes.flatMap(getJsonsWithPrefix(payload, _))
  }

  private def getJsonsWithPrefix(payload: JsonObject, prefix: String): List[JsonWithFlattenSchemaKey] =
    payload.filterKeys(_.startsWith(prefix)).toList.map {
      case (flattenSchemaKey, data) => JsonWithFlattenSchemaKey(flattenSchemaKey, data)
    }

  /**
    * Represents event which is reconstructed from payload of the failed insert
    * It consists of two part because schema keys of the self describing JSONs
    * such as unstruct_event, contexts or derived_contexts are flatten in the
    * payload. For example schema key with com.snowplowanalytics.snowplow as
    * vendor, web_page as name, 1.0.0 as version is converted to
    * "contexts_com_snowplowanalytics_snowplow_web_page_1_0_0" and this key could
    * not be reconverted to its original schema key deterministically. Therefore,
    * they can not be behaved like self describing JSONs after reconstruction because
    * their schema key are unknown. Therefore, reconstructed event split into two part
    * as atomic and JSONs with schema key
    * @param atomic event instance which only consists of atomic event parts
    * @param jsonsWithFlattenSchemaKey list of JsonWithFlattenSchemaKey instances
    *        they consist of reconstructed self describing JSONs from the payload
    */
  final case class ReconstructedEvent(atomic: Event, jsonsWithFlattenSchemaKey: List[JsonWithFlattenSchemaKey])

  implicit val reconstructedEventEncoder: Encoder[ReconstructedEvent] =
    Encoder.instance {
      case ReconstructedEvent(atomic, jsonsWithFlattenSchemaKey) =>
        Json.obj(
          "atomic" := atomic.asJson,
          "jsonsWithFlattenSchemaKey" := jsonsWithFlattenSchemaKey.asJson
        )
    }

  //TODO Enes: Ask Anton if we can give jsons of unstruct_event, contexts, derived_contexts
  // a general name, then we can change this class name to it
  /**
    * Represents JSON object with flatten schema key
    *
    * @param flattenSchemaKey flatten schema key e.g. "contexts_com_snowplowanalytics_snowplow_web_page_1_0_0"
    * @param data JSON object which can be unstruct_event, array of contexts or array of derived_contexts
    */
  final case class JsonWithFlattenSchemaKey(flattenSchemaKey: String, data: Json)

  implicit val jsonWithFlattenSchemaKeyEncoder: Encoder[JsonWithFlattenSchemaKey] =
    Encoder.instance {
      case JsonWithFlattenSchemaKey(flattenSchemaKey, data) =>
        Json.obj(
          "flattenSchemaKey" := flattenSchemaKey.asJson,
          "data" := data
        )
    }
}
