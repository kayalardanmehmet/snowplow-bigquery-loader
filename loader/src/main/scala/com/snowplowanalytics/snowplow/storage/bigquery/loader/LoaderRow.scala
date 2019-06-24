/*
 * Copyright (c) 2018 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.storage.bigquery
package loader

import org.joda.time.Instant
import cats.Id
import cats.data.ValidatedNel
import cats.implicits._
import io.circe.Json
import io.circe.syntax._
import com.spotify.scio.bigquery.TableRow
import com.snowplowanalytics.iglu.core.{SchemaKey, SelfDescribingData}
import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.schemaddl.jsonschema.{Schema => DdlSchema}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.circe.implicits._
import com.snowplowanalytics.iglu.schemaddl.bigquery.{Field, Row}
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.analytics.scalasdk.Data._
import common.{Adapter, Schema}
import IdInstances._
import loader.BadRow._

/** Row ready to be passed into Loader stream and Mutator topic */
case class LoaderRow(collectorTstamp: Instant, data: TableRow, inventory: Set[ShreddedType])

object LoaderRow {
  /**
    * Parse enriched TSV into a loader row, ready to be loaded into bigquery
    * If Loader is able to figure out that row cannot be loaded into BQ -
    * it will return return BadRow with detailed error that later can be analyzed
    * If premature check has passed, but row cannot be loaded it will be forwarded
    * to "failed inserts" topic, without additional information
    * @param resolver serializable Resolver's config to get it from singleton store
    * @param record enriched TSV line
    * @return either bad with error messages or entity ready to be loaded
    */
  def parse(resolver: Json)(record: String): Either[BadRow, LoaderRow] =
    for {
      event         <- Event.parse(record).toEither.leftMap(e => ParsingError(record, e))
      rowWithTstamp <- fromEvent(singleton.ResolverSingleton.get(resolver))(event)
      (row, tstamp)  = rowWithTstamp
    } yield LoaderRow(tstamp, row, event.inventory)

  type Transformed[A] = ValidatedNel[InternalErrorInfo, A]

  /** Parse JSON object provided by Snowplow Analytics SDK */
  def fromEvent(resolver: Resolver[Id])(event: Event): Either[InternalError, (TableRow, Instant)] = {
    val atomic: Transformed[List[(String, Any)]] = event
      .atomic
      .filter { case (key, value) => key == "geo_location" || !value.isNull }
      .toList
      .traverse[ValidatedNel[InternalErrorInfo, ?], (String, Any)] { case (key, value) =>
        value.fold(
          InternalErrorInfo.UnexpectedError(value, s"Unexpected JSON null in [$key]").invalidNel,   // Should not happen
          b => b.validNel,
          i => i.toInt.orElse(i.toBigDecimal).getOrElse(i.toDouble).validNel,
          s => s.validNel,
          _ => InternalErrorInfo.UnexpectedError(value, s"Unexpected JSON array with key [$key] found in EnrichedEvent").invalidNel,
          _ => InternalErrorInfo.UnexpectedError(value, s"Unexpected JSON object with key [$key] found in EnrichedEvent").invalidNel
        ).map { v => (key, v)}
      }

    val contexts: Transformed[List[(String, Any)]] = groupContexts(resolver, event.contexts.data.toVector)
    val derivedContexts: Transformed[List[(String, Any)]] = groupContexts(resolver, event.derived_contexts.data.toVector)
    val selfDescribingEvent: Transformed[List[(String, Any)]] = event.unstruct_event.data.map {
      case SelfDescribingData(schema, data) =>
        val columnName = Schema.getColumnName(ShreddedType(UnstructEvent, schema))
        transformJson(resolver)(schema)(data).map { row => List((columnName, Adapter.adaptRow(row))) }
    }.getOrElse(List.empty[(String, Any)].validNel)


    val result = (atomic, contexts, derivedContexts, selfDescribingEvent).mapN { (a, c, dc, e) =>
      val rows = a ++ c ++ dc ++ e
      (TableRow(rows: _*), new Instant(event.collector_tstamp.toEpochMilli) )
    }
    result.toEither.leftMap[InternalError](failureInfo => InternalError(event, failureInfo))
  }

  /** Group list of contexts by their full URI and transform values into ready to load rows */
  def groupContexts(resolver: Resolver[Id], contexts: Vector[SelfDescribingData[Json]]): ValidatedNel[InternalErrorInfo, List[(String, Any)]] = {
    val grouped = contexts.groupBy(_.schema).map { case (key, groupedContexts) =>
      val contexts = groupedContexts.map(_.data)    // Strip away URI
      val columnName = Schema.getColumnName(ShreddedType(Contexts(CustomContexts), key))
      val getRow = transformJson(resolver)(key)(_)
      contexts
        .toList
        .traverse[ValidatedNel[InternalErrorInfo, ?], Row](getRow)
        .map(rows => (columnName, Adapter.adaptRow(Row.Repeated(rows))))
    }
    grouped
      .toList
      .sequence[ValidatedNel[InternalErrorInfo, ?], (String, AnyRef)]
  }

  /**
    * Get BigQuery-compatible table rows from data-only JSON payload
    * Can be transformed to contexts (via Repeated) later only remain ue-compatible
    */
  def transformJson(resolver: Resolver[Id])(schemaKey: SchemaKey)(data: Json): ValidatedNel[InternalErrorInfo, Row] = {
    // TODO: proper stringify
    resolver.lookupSchema(schemaKey, 3)
      .leftMap(InternalErrorInfo.IgluLookupError(schemaKey, _))
      .flatMap(schema => DdlSchema.parse(schema).toRight(InternalErrorInfo.UnexpectedError(schemaKey.toSchemaUri.asJson, "Error while parsing ddl schema")))
      .map(schema => Field.build("", schema, false))
      .flatMap(Row.cast(_)(data).leftMap(InternalErrorInfo.CastError(data, schemaKey, _)).toEither)
      .toValidatedNel[InternalErrorInfo]
  }
}
