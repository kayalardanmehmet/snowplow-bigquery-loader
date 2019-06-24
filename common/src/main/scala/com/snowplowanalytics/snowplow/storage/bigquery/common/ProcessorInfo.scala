package com.snowplowanalytics.snowplow.storage.bigquery.common

import io.circe.{Encoder, Json}
import io.circe.syntax._
import com.snowplowanalytics.snowplow.storage.bigquery.generated.ProjectMetadata

object ProcessorInfo {
  val BQLoaderProcessorInfo = Processor(ProjectMetadata.name, ProjectMetadata.version)

  /**
    * Gives info about the processor where error occurred
    * @param artifact
    * @param version
    */
  final case class Processor(artifact: String, version: String)

  implicit val processorCirceJsonEncoder: Encoder[Processor] =
    Encoder.instance {
      case Processor(artifact, version) =>
        Json.obj(
          "artifact" := artifact.asJson,
          "version" := version.asJson
        )
    }
}
