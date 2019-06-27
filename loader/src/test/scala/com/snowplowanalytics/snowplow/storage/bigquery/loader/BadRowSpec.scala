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
package com.snowplowanalytics.snowplow.storage.bigquery.loader

import scala.reflect.{ClassTag, classTag}
import io.circe.literal._
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent._
import com.snowplowanalytics.snowplow.storage.bigquery.loader.BadRow.{InternalError, InternalErrorInfo, ParsingError}
import org.specs2.Specification
import org.specs2.matcher.{Expectable, MatchFailure, MatchResult, MatchSuccess, Matcher}

object BadRowSpec {

  val notEnrichedEvent = "Not enriched event"

  val resolverConfig = json"""
      {
         "schema":"iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-0",
         "data":{
            "cacheSize":500,
            "repositories":[
               {
                  "name":"Iglu Central",
                  "priority":0,
                  "vendorPrefixes":[
                     "com.snowplowanalytics"
                  ],
                  "connection":{
                     "http":{
                        "uri":"http://iglucentral.com"
                     }
                  }
               }
            ]
         }
      }
    """

  def beEventParsingErrorAndHaveSamePayload(record: String): Matcher[BadRow] = new Matcher[BadRow] {
    override def apply[S <: BadRow](t: Expectable[S]): MatchResult[S] = {
      t.value match {
        case ParsingError(`record`, _) =>
          MatchSuccess("Event parsing errors have same payload", "", t)
        case ParsingError(_, _) =>
          MatchFailure("", "Event parsing errors have different payload", t)
        case _ =>
          MatchFailure("", "Given bad row is not EventParsingError", t)
      }
    }
  }


  def beTypeOfErrorAndHaveSamePayload[T <: InternalErrorInfo: ClassTag](event: Event): Matcher[InternalError] = new Matcher[InternalError] {
    override def apply[S <: InternalError](t: Expectable[S]): MatchResult[S] = t.value match {
      case InternalError(`event`, errorInfos) =>
        val res = errorInfos.foldLeft(true)((res, e) => res & classTag[T].runtimeClass.isInstance(e))
        Matcher.result(
          res,
          "All the errors are IgluValidationError and payload is equal to given event",
          "All the errors are not IgluValidationError",
          t)
      case _ => MatchFailure("", "Given bad row is not BigQueryError", t)
    }
  }
}

class BadRowSpec extends Specification { def is = s2"""
  parsing not enriched event returns error $e1
  fromEvent returns IgluValidationError if event has contexts with not existing schemas $e2
  fromEvent returns CastError if event has missing fields with respect to its schema $e3
  test test $e4
  """
  import BadRowSpec._

  def e1 = {
    val res = LoaderRow.parse(BadRowSpec.resolverConfig)(notEnrichedEvent)
    res.swap.getOrElse(throw new RuntimeException("Result need to be left")) must beEventParsingErrorAndHaveSamePayload(notEnrichedEvent)
  }

  def e2 = {
    val contexts = List(
      SelfDescribingData(
        SchemaKey("com.snowplowanalytics.snowplow", "not_existing_context", "jsonschema", SchemaVer.Full(1, 0, 0)),
        json"""{"latitude": 22, "longitude": 23.1, "latitudeLongitudeAccuracy": 23} """
      )
    )
    val event = SpecHelpers.ExampleEvent.copy(contexts = Contexts(contexts), derived_contexts = Contexts(contexts))
    val res = LoaderRow.fromEvent(SpecHelpers.resolver)(event)
    res.swap.getOrElse(throw new RuntimeException("Result need to be left")) must beTypeOfErrorAndHaveSamePayload[InternalErrorInfo.IgluLookupError](event)
  }

  def e3 = {
    val contexts = List(
      SelfDescribingData(
        SchemaKey("com.snowplowanalytics.snowplow", "geolocation_context", "jsonschema", SchemaVer.Full(1, 0, 0)),
        json"""{}"""
      )
    )
    val event = SpecHelpers.ExampleEvent.copy(contexts = Contexts(contexts), derived_contexts = Contexts(contexts))
    val res = LoaderRow.fromEvent(SpecHelpers.resolver)(event)
    res.swap.getOrElse(throw new RuntimeException("Result need to be left")) must beTypeOfErrorAndHaveSamePayload[InternalErrorInfo.CastError](event)
  }

  def e4 = {
    val rawEvent = """snowplowweb     web     2018-12-18 15:07:17.970 2016-03-21 11:56:32.844 2016-03-21 11:56:31.811 page_ping       4fbd682f-3395-46dd-8aa0-ed0c1f5f1d92            snplow6 js-2.6.0        ssc-0.6.0-kinesis       spark-1.16.0-common-0.35.0      50d9ee525d12b25618e2dbab1f25c39d        086271e520deb84db3259c0af2878ee5        262ac8a29684b99251deb81d2e246446        9506c862a9dbeea2f55fba6af4a0e7ec        10      05d09faa707a9ff092e49929c79630d1                                                                                                http://snowplowanalytics.com/   Snowplow – Your digital nervous system          http    snowplowanalytics.com   80      /                                                                                                                                       {"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0","data":[{"schema":"iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0","data":{"id":"3e4a5deb-1504-4cb2-8751-9d358c632328"}},{"schema":"iglu:org.w3/PerformanceTiming/jsonschema/1-0-0","data":{"navigationStart":1458561378194,"unloadEventStart":1458561379080,"unloadEventEnd":1458561379080,"redirectStart":0,"redirectEnd":0,"fetchStart":1458561378195,"domainLookupStart":1458561378211,"domainLookupEnd":1458561378435,"connectStart":1458561378435,"connectEnd":1458561378602,"secureConnectionStart":0,"requestStart":1458561378602,"responseStart":1458561379079,"responseEnd":1458561379080,"domLoading":1458561379095,"domInteractive":1458561380392,"domContentLoadedEventStart":1458561380392,"domContentLoadedEventEnd":1458561380395,"domComplete":1458561383683,"loadEventStart":1458561383683,"loadEventEnd":1458561383737,"chromeFirstPaint":1458561380397}}]}                                                                                                                                                                 0       0       1786    2123    Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.87 Safari/537.36 BMID/E6797F732B        Chrome 49       Chrome  49.0.2623.87    Browser WEBKIT  en-US   1       1       0       0       0       0       0       0       0       1       24      1591    478     Mac OS X        Mac OS X        Apple Inc.      Europe/London   Computer        0       1680    1050    UTF-8   1591    3017                                                                                            2016-03-21 11:56:31.813                 {"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:com.snowplowanalytics.snowplow/ua_parser_context/jsonschema/1-0-0","data":{"useragentFamily":"Chrome","useragentMajor":"49","useragentMinor":"0","useragentPatch":"2623","useragentVersion":"Chrome 49.0.2623","osFamily":"Mac OS X","osMajor":"10","osMinor":"10","osPatch":"5","osPatchMinor":null,"osVersion":"Mac OS X 10.10.5","deviceFamily":"Other"}},{"schema":"iglu:org.ietf/http_cookie/jsonschema/1-0-0","data":{"name":"sp","value":"05d09faa707a9ff092e49929c79630d1"}},{"schema":"iglu:org.ietf/http_cookie/jsonschema/1-0-0","data":{"name":"sp","value":"b73833bf7e3af5a6d5a02e79be96b064"}}]}        bfcfa2c3-9b9e-4023-a850-9873e52e0fd2    2016-03-21 11:56:32.842 com.snowplowanalytics.snowplow  page_ping       jsonschema      1-0-0   1be0be1f32b2854847f96bb83479ea44"""
    val context = SelfDescribingData(
      SchemaKey("com.snowplowanalytics.snowplow", "geolocation_context", "jsonschema", SchemaVer.Full(1, 0, 0)),
      json"""{"latitude": 22, "longitude": 23.1, "latitudeLongitudeAccuracy": 23} """
    )
    val contexts = List(context)
    val event = SpecHelpers.ExampleEvent.copy(contexts = Contexts(contexts), derived_contexts = Contexts(contexts), unstruct_event = UnstructEvent(Some(context)))
    val res = LoaderRow.fromEvent(SpecHelpers.resolver)(event)
    println(res)
    val res2 = LoaderRow.parse(BadRowSpec.resolverConfig)(rawEvent)
    println(res2)
    ok
  }
}
