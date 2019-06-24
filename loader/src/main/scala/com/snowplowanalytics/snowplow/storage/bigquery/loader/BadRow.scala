/*
 * Copyright (c) 2019 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.storage.bigquery.loader

import cats.data.NonEmptyList
import io.circe.{Encoder, Json}
import io.circe.syntax._
import com.snowplowanalytics.iglu.client.ClientError
import com.snowplowanalytics.iglu.schemaddl.bigquery.{CastError => BigQueryCastError}
import com.snowplowanalytics.iglu.core.{SchemaKey, SelfDescribingData}
import com.snowplowanalytics.iglu.core.circe.instances._
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.storage.bigquery.common.{BadRowSchemas, ProcessorInfo}


/**
  * Represents events which are problematic to process due to
  * several reasons such as could not be parsed, error while validate
  * them with Iglu, error while casting them to BigQuery types etc.
  */
sealed trait BadRow {
  def getSelfDescribingData: SelfDescribingData[Json]
  def compact: String  = getSelfDescribingData.asJson.noSpaces
}

object BadRow {
  /**
    * Represents the failure case where data can not be parsed as a proper event
    *
    * @param payload data blob tried to be parsed
    * @param errors  errors in the end of the parsing process
    */
  final case class ParsingError(payload: String, errors: NonEmptyList[String]) extends BadRow {
    def getSelfDescribingData: SelfDescribingData[Json] =
      SelfDescribingData(BadRowSchemas.LoaderParsingError, (this: BadRow).asJson)
  }

  /**
    * Represents the errors due to something went wrong internally
    * In this kind of errors, event is parsed properly, therefore
    * enriched event is given in the error as properly parsed
    *
    * @param enrichedEvent event which is enriched successfully
    * @param errors        info of errors
    */
  final case class InternalError(enrichedEvent: Event, errors: NonEmptyList[InternalErrorInfo]) extends BadRow {
    def getSelfDescribingData: SelfDescribingData[Json] =
      SelfDescribingData(BadRowSchemas.LoaderInternalError, (this: BadRow).asJson)
  }

  implicit val badRowCirceJsonEncoder: Encoder[BadRow] =
    Encoder.instance {
      case ParsingError(payload, errors) =>
        Json.obj(
          "payload" := payload.asJson,
          "errors" := errors.asJson,
          "processor" := ProcessorInfo.BQLoaderProcessorInfo.asJson
        )
      case InternalError(enrichedEvent, errors) =>
        Json.obj(
          "enrichedEvent" := enrichedEvent.asJson,
          "failures" := errors.asJson,
          "processor" := ProcessorInfo.BQLoaderProcessorInfo.asJson
        )
    }

  /**
    * Gives info about the reasons of the internal errors
    */
  sealed trait InternalErrorInfo

  object InternalErrorInfo {

    /**
      * Represents errors which occurs when looked up schema
      * could not be found in the given Iglu registries
      *
      * @param schemaKey instance of schemaKey which could not be found
      * @param error     instance of ClientError which gives info
      *                  about the reason of lookup error
      */
    final case class IgluLookupError(schemaKey: SchemaKey, error: ClientError) extends InternalErrorInfo

    /**
      * Represents errors which occurs when trying to turn JSON value
      * into BigQuery-compatible row
      *
      * @param data      JSON data which tried to be casted
      * @param schemaKey key of schema which 'Field' instance to cast is created from
      * @param errors    info about reason of the error
      */
    final case class CastError(data: Json, schemaKey: SchemaKey, errors: NonEmptyList[BigQueryCastError]) extends InternalErrorInfo

    final case class UnexpectedError(value: Json, message: String) extends InternalErrorInfo

    implicit val errorInfoJsonEncoder: Encoder[InternalErrorInfo] =
      Encoder.instance {
        case IgluLookupError(schemaKey, error) =>
          Json.obj("igluValidationError" :=
            Json.obj(
              "schemaKey" := schemaKey.asJson,
              "error" := error.asJson
            )
          )
        case CastError(data, schemaKey, errors) =>
          Json.obj("castError" :=
            Json.obj(
              "data" := data,
              "schemaKey" := schemaKey.asJson,
              "errors" := errors.asJson
            )
          )
        case UnexpectedError(value, message) =>
          Json.obj("unexpectedError" :=
            Json.obj(
              "value" := value,
              "message" := message.asJson
            )
          )
      }

    implicit val bigQueryCastErrorJsonEncoder: Encoder[BigQueryCastError] =
      Encoder.instance {
        case BigQueryCastError.WrongType(value, expected) =>
          Json.fromFields(List(
            ("message", Json.fromString("Unexpected type of value")),
            ("value", value),
            ("expectedType", Json.fromString(expected.toString))
          ))
        case BigQueryCastError.NotAnArray(value, expected) =>
          Json.fromFields(List(
            ("message", Json.fromString("Value should be in array")),
            ("value", value),
            ("expectedType", Json.fromString(expected.toString))
          ))
        case BigQueryCastError.MissingInValue(key, value) =>
          Json.fromFields(List(
            ("message", Json.fromString("Key is missing in value")),
            ("value", value),
            ("missingKey", Json.fromString(key))
          ))
      }
  }
}
