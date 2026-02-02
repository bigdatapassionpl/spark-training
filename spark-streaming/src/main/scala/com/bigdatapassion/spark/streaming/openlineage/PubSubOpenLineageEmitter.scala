package com.bigdatapassion.spark.streaming.openlineage

import com.google.cloud.pubsub.v1.SchemaServiceClient
import com.google.pubsub.v1.SchemaName
import io.openlineage.client.OpenLineage
import io.openlineage.client.transports.{ConsoleTransport, FileTransport, HttpTransport, FileConfig, HttpConfig}
import org.apache.avro.Schema

import java.net.URI
import java.time.ZonedDateTime
import java.util.UUID
import scala.jdk.CollectionConverters._

/**
 * Emits custom OpenLineage events with GCP Pub/Sub Schema Registry metadata.
 * This provides full schema information from GCP Schema Registry in the lineage events.
 */
object PubSubOpenLineageEmitter {

  private val producer = "https://github.com/bigdatapassion/spark-training"

  // Lazy-initialized transports
  private var fileTransport: FileTransport = _
  private var httpTransport: HttpTransport = _
  private var consoleTransport: ConsoleTransport = _

  // Cached schema
  private var cachedSchemaJson: String = _
  private var cachedSchemaId: String = _

  /**
   * Emits an OpenLineage START event with GCP Schema Registry metadata.
   */
  def emitStartEvent(
    gcpProject: String,
    schemaId: String,
    subscriptionId: String,
    namespace: String,
    jobName: String,
    openlineageFileLocation: String,
    marquezUrl: String = "http://localhost:5050"
  ): UUID = {

    // Fetch schema from GCP Schema Registry
    val (schemaJson, schemaName) = fetchSchemaFromRegistry(gcpProject, schemaId)

    val openLineage = new OpenLineage(URI.create(producer))
    val runId = UUID.randomUUID()

    // Parse Avro schema to extract fields for OpenLineage schema facet
    val schemaFields = new java.util.ArrayList[OpenLineage.SchemaDatasetFacetFields]()
    val avroSchema = new Schema.Parser().parse(schemaJson)
    addSchemaFields(openLineage, schemaFields, avroSchema, "")

    val schemaFacet = openLineage.newSchemaDatasetFacetBuilder()
      .fields(schemaFields)
      .build()

    // Create input dataset with full schema from GCP Schema Registry
    val inputDataset = openLineage.newInputDatasetBuilder()
      .namespace(s"pubsub://$gcpProject")
      .name(subscriptionId)
      .facets(
        openLineage.newDatasetFacetsBuilder()
          .schema(schemaFacet)
          .dataSource(
            openLineage.newDatasourceDatasetFacetBuilder()
              .name(s"pubsub://$gcpProject")
              .uri(URI.create(s"pubsub://$gcpProject/subscriptions/$subscriptionId"))
              .build()
          )
          .build()
      )
      .build()

    val inputs = java.util.Arrays.asList(inputDataset)

    // Create job with schema registry info in description
    val job = openLineage.newJobBuilder()
      .namespace(namespace)
      .name(jobName)
      .facets(
        openLineage.newJobFacetsBuilder()
          .jobType(
            openLineage.newJobTypeJobFacetBuilder()
              .processingType("STREAMING")
              .integration("SPARK")
              .jobType("APPLICATION")
              .build()
          )
          .documentation(
            openLineage.newDocumentationJobFacetBuilder()
              .description(
                s"""GCP Pub/Sub Schema Registry Integration:
                   |  Project: $gcpProject
                   |  Schema: $schemaName
                   |  Schema ID: $schemaId
                   |  Subscription: $subscriptionId
                   |""".stripMargin
              )
              .build()
          )
          .build()
      )
      .build()

    val run = openLineage.newRunBuilder()
      .runId(runId)
      .facets(openLineage.newRunFacetsBuilder().build())
      .build()

    val startEvent = openLineage.newRunEventBuilder()
      .eventType(OpenLineage.RunEvent.EventType.START)
      .eventTime(ZonedDateTime.now())
      .run(run)
      .job(job)
      .inputs(inputs)
      .build()

    emitEvent(startEvent, openlineageFileLocation, marquezUrl)

    println("=" * 80)
    println("OPENLINEAGE START EVENT EMITTED WITH GCP SCHEMA REGISTRY METADATA")
    println("=" * 80)
    println(s"Run ID: $runId")
    println(s"GCP Project: $gcpProject")
    println(s"Schema ID: $schemaId")
    println(s"Subscription: $subscriptionId")
    println(s"Fields in schema: ${schemaFields.size()}")
    println("=" * 80)

    runId
  }

  /**
   * Emits an OpenLineage RUNNING event with current schema from GCP Schema Registry.
   */
  def emitRunningEventWithSchema(
    runId: UUID,
    gcpProject: String,
    schemaId: String,
    subscriptionId: String,
    namespace: String,
    jobName: String,
    openlineageFileLocation: String,
    batchId: Long,
    recordCount: Long,
    marquezUrl: String = "http://localhost:5050"
  ): Unit = {

    // Fetch schema from GCP Schema Registry (uses cache if available)
    val (schemaJson, schemaName) = fetchSchemaFromRegistry(gcpProject, schemaId)

    val openLineage = new OpenLineage(URI.create(producer))

    // Parse Avro schema to extract fields
    val schemaFields = new java.util.ArrayList[OpenLineage.SchemaDatasetFacetFields]()
    val avroSchema = new Schema.Parser().parse(schemaJson)
    addSchemaFields(openLineage, schemaFields, avroSchema, "")

    val schemaFacet = openLineage.newSchemaDatasetFacetBuilder()
      .fields(schemaFields)
      .build()

    // Create input dataset with current schema
    val inputDataset = openLineage.newInputDatasetBuilder()
      .namespace(s"pubsub://$gcpProject")
      .name(subscriptionId)
      .facets(
        openLineage.newDatasetFacetsBuilder()
          .schema(schemaFacet)
          .dataSource(
            openLineage.newDatasourceDatasetFacetBuilder()
              .name(s"pubsub://$gcpProject")
              .uri(URI.create(s"pubsub://$gcpProject/subscriptions/$subscriptionId"))
              .build()
          )
          .build()
      )
      .build()

    val inputs = java.util.Arrays.asList(inputDataset)

    val job = openLineage.newJobBuilder()
      .namespace(namespace)
      .name(jobName)
      .facets(
        openLineage.newJobFacetsBuilder()
          .jobType(
            openLineage.newJobTypeJobFacetBuilder()
              .processingType("STREAMING")
              .integration("SPARK")
              .jobType("APPLICATION")
              .build()
          )
          .documentation(
            openLineage.newDocumentationJobFacetBuilder()
              .description(
                s"""GCP Pub/Sub Schema Registry Integration (Batch $batchId):
                   |  Project: $gcpProject
                   |  Schema: $schemaName
                   |  Schema ID: $schemaId
                   |  Subscription: $subscriptionId
                   |  Records in batch: $recordCount
                   |""".stripMargin
              )
              .build()
          )
          .build()
      )
      .build()

    val run = openLineage.newRunBuilder()
      .runId(runId)
      .facets(openLineage.newRunFacetsBuilder().build())
      .build()

    val runningEvent = openLineage.newRunEventBuilder()
      .eventType(OpenLineage.RunEvent.EventType.RUNNING)
      .eventTime(ZonedDateTime.now())
      .run(run)
      .job(job)
      .inputs(inputs)
      .build()

    emitEvent(runningEvent, openlineageFileLocation, marquezUrl)

    println(s"[Batch $batchId] Emitted RUNNING event - Schema: $schemaId, Records: $recordCount")
  }

  /**
   * Emits an OpenLineage COMPLETE event with final schema from GCP Schema Registry.
   */
  def emitCompleteEvent(
    runId: UUID,
    gcpProject: String,
    schemaId: String,
    subscriptionId: String,
    namespace: String,
    jobName: String,
    openlineageFileLocation: String,
    marquezUrl: String = "http://localhost:5050"
  ): Unit = {

    val openLineage = new OpenLineage(URI.create(producer))

    // Include schema in COMPLETE event if available
    val inputs = if (cachedSchemaJson != null) {
      val schemaFields = new java.util.ArrayList[OpenLineage.SchemaDatasetFacetFields]()
      val avroSchema = new Schema.Parser().parse(cachedSchemaJson)
      addSchemaFields(openLineage, schemaFields, avroSchema, "")

      val schemaFacet = openLineage.newSchemaDatasetFacetBuilder()
        .fields(schemaFields)
        .build()

      val inputDataset = openLineage.newInputDatasetBuilder()
        .namespace(s"pubsub://$gcpProject")
        .name(subscriptionId)
        .facets(
          openLineage.newDatasetFacetsBuilder()
            .schema(schemaFacet)
            .build()
        )
        .build()

      java.util.Arrays.asList(inputDataset)
    } else {
      null
    }

    val run = openLineage.newRunBuilder()
      .runId(runId)
      .build()

    val job = openLineage.newJobBuilder()
      .namespace(namespace)
      .name(jobName)
      .build()

    val completeEventBuilder = openLineage.newRunEventBuilder()
      .eventType(OpenLineage.RunEvent.EventType.COMPLETE)
      .eventTime(ZonedDateTime.now())
      .run(run)
      .job(job)

    if (inputs != null) {
      completeEventBuilder.inputs(inputs)
    }

    emitEvent(completeEventBuilder.build(), openlineageFileLocation, marquezUrl)
  }

  private def fetchSchemaFromRegistry(gcpProject: String, schemaId: String): (String, String) = {
    if (cachedSchemaJson == null || cachedSchemaId != schemaId) {
      val schemaName = SchemaName.of(gcpProject, schemaId)
      val schemaServiceClient = SchemaServiceClient.create()
      try {
        val gcpSchema = schemaServiceClient.getSchema(schemaName)
        cachedSchemaJson = gcpSchema.getDefinition
        cachedSchemaId = schemaId
      } finally {
        schemaServiceClient.close()
      }
    }
    (cachedSchemaJson, s"projects/$gcpProject/schemas/$schemaId")
  }

  private def addSchemaFields(
    openLineage: OpenLineage,
    fields: java.util.ArrayList[OpenLineage.SchemaDatasetFacetFields],
    schema: Schema,
    prefix: String
  ): Unit = {

    schema.getType match {
      case Schema.Type.RECORD =>
        schema.getFields.asScala.foreach { field =>
          val fieldName = if (prefix.isEmpty) field.name() else s"$prefix.${field.name()}"
          val fieldSchema = field.schema()

          // Handle union types (nullable fields)
          val actualSchema = if (fieldSchema.getType == Schema.Type.UNION) {
            fieldSchema.getTypes.asScala.find(_.getType != Schema.Type.NULL).getOrElse(fieldSchema)
          } else {
            fieldSchema
          }

          if (actualSchema.getType == Schema.Type.RECORD) {
            // Recursively add nested record fields
            addSchemaFields(openLineage, fields, actualSchema, fieldName)
          } else {
            fields.add(
              openLineage.newSchemaDatasetFacetFieldsBuilder()
                .name(fieldName)
                .`type`(actualSchema.getType.getName)
                .description(Option(field.doc()).getOrElse(""))
                .build()
            )
          }
        }
      case _ =>
        // Non-record type at top level
        fields.add(
          openLineage.newSchemaDatasetFacetFieldsBuilder()
            .name(if (prefix.isEmpty) "value" else prefix)
            .`type`(schema.getType.getName)
            .build()
        )
    }
  }

  private def getFileTransport(fileLocation: String): FileTransport = {
    if (fileTransport == null) {
      val fileConfig = new FileConfig()
      fileConfig.setLocation(fileLocation)
      fileTransport = new FileTransport(fileConfig)
    }
    fileTransport
  }

  private def getHttpTransport(marquezUrl: String): HttpTransport = {
    if (httpTransport == null) {
      val httpConfig = new HttpConfig()
      httpConfig.setUrl(URI.create(marquezUrl))
      httpConfig.setEndpoint("api/v1/lineage")
      httpTransport = new HttpTransport(httpConfig)
    }
    httpTransport
  }

  private def getConsoleTransport(): ConsoleTransport = {
    if (consoleTransport == null) {
      consoleTransport = new ConsoleTransport()
    }
    consoleTransport
  }

  private def emitEvent(event: OpenLineage.RunEvent, fileLocation: String, marquezUrl: String): Unit = {
    // Emit to all transports
    getFileTransport(fileLocation).emit(event)
    getHttpTransport(marquezUrl).emit(event)
    getConsoleTransport().emit(event)
  }
}
