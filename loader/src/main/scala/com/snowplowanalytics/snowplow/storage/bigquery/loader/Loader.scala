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

import com.google.api.services.bigquery.model.TableReference
import org.joda.time.Duration
import org.json4s.jackson.JsonMethods.compact
import com.spotify.scio.ScioContext
import com.spotify.scio.values.{SCollection, SideOutput, WindowOptions}
import org.apache.beam.sdk.transforms.windowing._
import org.apache.beam.sdk.io.gcp.bigquery.{BigQueryIO, InsertRetryPolicy}
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{CreateDisposition, WriteDisposition}
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import com.snowplowanalytics.snowplow.analytics.scalasdk.json.Data.InventoryItem
import common.Config._
import common.Codecs.toPayload
import org.json4s.JsonAST.JValue
import com.redis._
import org.slf4j.Logger
import org.slf4j.LoggerFactory


object Loader {

  val logger = LoggerFactory.getLogger("Loader")
  val rClients = new RedisClientPool("10.128.15.238", 6379)

  val OutputWindow: Duration =
    Duration.millis(10000)

  val OutputWindowOptions = WindowOptions(
    Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()),
    AccumulationMode.DISCARDING_FIRED_PANES,
    Duration.ZERO)

  /** Side output for shredded types */
  val ObservedTypesOutput: SideOutput[Set[InventoryItem]] =
    SideOutput[Set[InventoryItem]]()

  /** Emit observed types every 10 minutes / workers */
  val TypesWindow: Duration =
    Duration.millis(60000)

  /** Side output for rows failed transformation */
  val BadRowsOutput: SideOutput[BadRow] =
    SideOutput[BadRow]()

  /** Run whole pipeline */
  def run(env: Environment, sc: ScioContext): Unit = {
    val (mainOutput, sideOutputs) = getData(env.resolverJson, sc, env.config.getFullInput)
      .withSideOutputs(ObservedTypesOutput, BadRowsOutput)
      .withName("splitGoodBad")
      .flatMap {
        case (Right(row), ctx) =>

          if(row.data != null && row.data.get("event_fingerprint") != null){
            val fingerprint = row.data.get("event_fingerprint").asInstanceOf[String]
            if (fingerprint != null){

              try {
                rClients.withClient {
                  client => {
                    val resp = client.get("fp-" + fingerprint)
                    if (resp.getOrElse("-1").equals("-1")) {
                      client.setex("fp-" + fingerprint, 86400, "1")
                      ctx.output(ObservedTypesOutput, row.inventory)
                      Some(row)
                    }else{
                      logger.warn("Duplicated event detected fingerprint: " + fingerprint)
                      None
                    }
                  }
                }
              }catch {
                case err: RedisConnectionException => {
                  logger.error("redis connection error: " + err.message)
                  ctx.output(ObservedTypesOutput, row.inventory)
                  Some(row)
                }
                case err: Throwable => {
                  logger.error("error: " + err.getMessage)
                  ctx.output(ObservedTypesOutput, row.inventory)
                  Some(row)
                }
              }

            }else{
              logger.warn("fingerprint null")
              ctx.output(ObservedTypesOutput, row.inventory)
              Some(row)
            }
          }else{
            logger.warn("data null")
            ctx.output(ObservedTypesOutput, row.inventory)
            Some(row)
          }
        case (Left(row), ctx) =>
          ctx.output(BadRowsOutput, row)
          None
      }

    // Emit all types observed in 1 minute
    aggregateTypes(sideOutputs(ObservedTypesOutput))
      .saveAsPubsub(env.config.getFullTypesTopic)

    // Sink bad rows
    sideOutputs(BadRowsOutput)
      .map(_.compact)
      .saveAsPubsub(env.config.getFullBadRowsTopic)

    // Unwrap LoaderRow collection to get failed inserts
    val mainOutputInternal = mainOutput.internal
    val remaining = getOutput(env.config.load)
      .to(getTableReference(env))
      .expand(mainOutputInternal).getFailedInserts

    // Sink good rows and forward failed inserts to PubSub
    sc.wrap(remaining)
      .withName("failedInsertsSink")
      .saveAsPubsub(env.config.getFullFailedInsertsTopic)
  }

  /** Default BigQuery output options */
  def getOutput(loadMode: LoadMode): BigQueryIO.Write[LoaderRow] = {
    val common =
      BigQueryIO.write()
        .withFormatFunction(SerializeLoaderRow)
        .withCreateDisposition(CreateDisposition.CREATE_NEVER)
        .withWriteDisposition(WriteDisposition.WRITE_APPEND)

    loadMode match {
      case LoadMode.StreamingInserts(retry) =>
        val streaming = common.withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
        if (retry) streaming.withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
        else streaming.withFailedInsertRetryPolicy(InsertRetryPolicy.neverRetry())
      case LoadMode.FileLoads(frequency) =>
        common
          .withMethod(BigQueryIO.Write.Method.FILE_LOADS)
          .withNumFileShards(100)
          .withTriggeringFrequency(Duration.standardSeconds(frequency))
    }
  }

  /** Group types into smaller chunks */
  def aggregateTypes(types: SCollection[Set[InventoryItem]]): SCollection[String] =
    types
      .withFixedWindows(TypesWindow, options = OutputWindowOptions)
      .withName("aggregateTypes")
      .aggregate(Set.empty[InventoryItem])(_ ++ _, _ ++ _)
      .withName("withIntervalWindow")
      .withWindow[IntervalWindow]
      .swap
      .groupByKey
      .map { case (_, groupedSets) => groupedSets.toSet.flatten }
      .withName("filterNonEmpty")
      .filter(_.nonEmpty)
      .map { types => compact(toPayload(types)) }

  /** Read data from PubSub topic and transform to ready to load rows */
  def getData(resolver: JValue,
              sc: ScioContext,
              input: String): SCollection[Either[BadRow, LoaderRow]] =
    sc.pubsubSubscription[String](input)
      .map(LoaderRow.parse(resolver))
      .withFixedWindows(OutputWindow, options = OutputWindowOptions)

  def getTableReference(env: Environment): TableReference =
    new TableReference()
      .setProjectId(env.config.projectId)
      .setDatasetId(env.config.datasetId)
      .setTableId(env.config.tableId)
}
