package com.bigdatapassion.spark.streaming.openlineage

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient

/**
 * Custom facet containing Schema Registry metadata.
 * This will be serialized as part of the OpenLineage event JSON.
 */
case class SchemaRegistryInfo(
  schemaRegistryUrl: String,
  subject: String,
  schemaId: Int,
  schemaVersion: Int,
  schemaType: String,
  schema: String
)

/**
 * Helper object to fetch and cache Schema Registry metadata for OpenLineage integration.
 */
object SchemaRegistryOpenLineageHelper {

  private var schemaRegistryClient: CachedSchemaRegistryClient = _
  private var cachedSchemaInfo: Map[String, SchemaRegistryInfo] = Map.empty

  def getSchemaInfo(schemaRegistryUrl: String, topic: String): Option[SchemaRegistryInfo] = {
    val cacheKey = s"$schemaRegistryUrl:$topic"

    cachedSchemaInfo.get(cacheKey).orElse {
      try {
        if (schemaRegistryClient == null) {
          schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 100)
        }

        val subject = s"$topic-value"
        val metadata = schemaRegistryClient.getLatestSchemaMetadata(subject)

        val info = SchemaRegistryInfo(
          schemaRegistryUrl = schemaRegistryUrl,
          subject = subject,
          schemaId = metadata.getId,
          schemaVersion = metadata.getVersion,
          schemaType = metadata.getSchemaType,
          schema = metadata.getSchema
        )

        cachedSchemaInfo = cachedSchemaInfo + (cacheKey -> info)
        Some(info)
      } catch {
        case e: Exception =>
          println(s"Failed to fetch schema from registry: ${e.getMessage}")
          None
      }
    }
  }

  /**
   * Creates a JSON string representing the schema registry facet
   * that can be added to OpenLineage events.
   */
  def toOpenLineageFacetJson(info: SchemaRegistryInfo): String = {
    s"""{
       |  "_producer": "https://github.com/bigdatapassion/spark-training",
       |  "_schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$$defs/InputDatasetFacet",
       |  "schemaRegistry": {
       |    "url": "${info.schemaRegistryUrl}",
       |    "subject": "${info.subject}",
       |    "schemaId": ${info.schemaId},
       |    "schemaVersion": ${info.schemaVersion},
       |    "schemaType": "${info.schemaType}"
       |  },
       |  "avroSchema": ${info.schema}
       |}""".stripMargin
  }
}
