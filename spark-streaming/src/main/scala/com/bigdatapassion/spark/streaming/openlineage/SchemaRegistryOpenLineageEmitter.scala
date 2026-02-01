package com.bigdatapassion.spark.streaming.openlineage

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.openlineage.client.OpenLineage
import io.openlineage.client.transports.{ConsoleTransport, FileTransport, HttpTransport, FileConfig, HttpConfig}

import java.net.URI
import java.time.ZonedDateTime
import java.util.UUID

/**
 * Emits custom OpenLineage events with Schema Registry metadata.
 * This provides full schema registry information in the lineage events.
 * Uses OpenLineage client with composite transport (file + HTTP + console).
 */
object SchemaRegistryOpenLineageEmitter {

  private val producer = "https://github.com/bigdatapassion/spark-training"
  private var schemaRegistryClient: CachedSchemaRegistryClient = _

  /**
   * Emits an OpenLineage START event with Schema Registry metadata in the schema facet.
   */
  def emitSchemaRegistryLineageEvent(
    schemaRegistryUrl: String,
    topic: String,
    namespace: String,
    jobName: String,
    openlineageFileLocation: String,
    marquezUrl: String = "http://localhost:5050"
  ): UUID = {

    // Fetch schema from registry
    if (schemaRegistryClient == null) {
      schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 100)
    }

    val subject = s"$topic-value"
    val metadata = schemaRegistryClient.getLatestSchemaMetadata(subject)

    // Create OpenLineage client
    val openLineage = new OpenLineage(URI.create(producer))

    // Create run ID
    val runId = UUID.randomUUID()

    // Parse Avro schema to extract fields for OpenLineage schema facet
    val schemaFields = new java.util.ArrayList[OpenLineage.SchemaDatasetFacetFields]()
    val avroSchema = new org.apache.avro.Schema.Parser().parse(metadata.getSchema)

    // Recursively add fields including nested ones
    addSchemaFields(openLineage, schemaFields, avroSchema, "")

    val schemaFacet = openLineage.newSchemaDatasetFacetBuilder()
      .fields(schemaFields)
      .build()

    // Create input dataset with full schema from Schema Registry
    val inputDataset = openLineage.newInputDatasetBuilder()
      .namespace(s"kafka://localhost:9092")
      .name(topic)
      .facets(
        openLineage.newDatasetFacetsBuilder()
          .schema(schemaFacet)
          .dataSource(
            openLineage.newDatasourceDatasetFacetBuilder()
              .name(s"kafka://localhost:9092")
              .uri(URI.create(s"kafka://localhost:9092/$topic"))
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
                s"""Schema Registry Integration:
                   |  URL: $schemaRegistryUrl
                   |  Subject: $subject
                   |  Schema ID: ${metadata.getId}
                   |  Schema Version: ${metadata.getVersion}
                   |  Schema Type: ${metadata.getSchemaType}
                   |""".stripMargin
              )
              .build()
          )
          .build()
      )
      .build()

    // Create run
    val run = openLineage.newRunBuilder()
      .runId(runId)
      .facets(openLineage.newRunFacetsBuilder().build())
      .build()

    // Create START event
    val startEvent = openLineage.newRunEventBuilder()
      .eventType(OpenLineage.RunEvent.EventType.START)
      .eventTime(ZonedDateTime.now())
      .run(run)
      .job(job)
      .inputs(inputs)
      .build()

    // Emit via OpenLineage transport (file + Marquez + console)
    emitEvent(startEvent, openlineageFileLocation, marquezUrl)

    println("=" * 80)
    println("OPENLINEAGE EVENT EMITTED WITH SCHEMA REGISTRY METADATA")
    println("=" * 80)
    println(s"Run ID: $runId")
    println(s"Schema Registry URL: $schemaRegistryUrl")
    println(s"Subject: $subject")
    println(s"Schema ID: ${metadata.getId}")
    println(s"Schema Version: ${metadata.getVersion}")
    println(s"Schema Type: ${metadata.getSchemaType}")
    println(s"Fields in schema: ${schemaFields.size()}")
    println("=" * 80)

    runId
  }

  private def addSchemaFields(
    openLineage: OpenLineage,
    fields: java.util.ArrayList[OpenLineage.SchemaDatasetFacetFields],
    schema: org.apache.avro.Schema,
    prefix: String
  ): Unit = {
    import scala.jdk.CollectionConverters._

    schema.getType match {
      case org.apache.avro.Schema.Type.RECORD =>
        schema.getFields.asScala.foreach { field =>
          val fieldName = if (prefix.isEmpty) field.name() else s"$prefix.${field.name()}"
          val fieldSchema = field.schema()

          // Handle union types (nullable fields)
          val actualSchema = if (fieldSchema.getType == org.apache.avro.Schema.Type.UNION) {
            fieldSchema.getTypes.asScala.find(_.getType != org.apache.avro.Schema.Type.NULL).getOrElse(fieldSchema)
          } else {
            fieldSchema
          }

          if (actualSchema.getType == org.apache.avro.Schema.Type.RECORD) {
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

  // Lazy-initialized transports
  private var fileTransport: FileTransport = _
  private var httpTransport: HttpTransport = _
  private var consoleTransport: ConsoleTransport = _

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

  /**
   * Emits an OpenLineage RUNNING event with current schema from Schema Registry.
   * Use this to track schema changes during streaming execution.
   */
  def emitRunningEventWithSchema(
    runId: UUID,
    schemaRegistryUrl: String,
    topic: String,
    namespace: String,
    jobName: String,
    openlineageFileLocation: String,
    batchId: Long,
    recordCount: Long,
    marquezUrl: String = "http://localhost:5050"
  ): Unit = {
    // Fetch current schema from registry (may have changed since start)
    if (schemaRegistryClient == null) {
      schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 100)
    }

    val subject = s"$topic-value"
    val metadata = schemaRegistryClient.getLatestSchemaMetadata(subject)

    val openLineage = new OpenLineage(URI.create(producer))

    // Parse Avro schema to extract fields
    val schemaFields = new java.util.ArrayList[OpenLineage.SchemaDatasetFacetFields]()
    val avroSchema = new org.apache.avro.Schema.Parser().parse(metadata.getSchema)
    addSchemaFields(openLineage, schemaFields, avroSchema, "")

    val schemaFacet = openLineage.newSchemaDatasetFacetBuilder()
      .fields(schemaFields)
      .build()

    // Create input dataset with current schema
    val inputDataset = openLineage.newInputDatasetBuilder()
      .namespace(s"kafka://localhost:9092")
      .name(topic)
      .facets(
        openLineage.newDatasetFacetsBuilder()
          .schema(schemaFacet)
          .dataSource(
            openLineage.newDatasourceDatasetFacetBuilder()
              .name(s"kafka://localhost:9092")
              .uri(URI.create(s"kafka://localhost:9092/$topic"))
              .build()
          )
          .build()
      )
      .inputFacets(
        openLineage.newInputDatasetInputFacetsBuilder()
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
                s"""Schema Registry Integration (Batch $batchId):
                   |  URL: $schemaRegistryUrl
                   |  Subject: $subject
                   |  Schema ID: ${metadata.getId}
                   |  Schema Version: ${metadata.getVersion}
                   |  Schema Type: ${metadata.getSchemaType}
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

    println(s"[Batch $batchId] Emitted RUNNING event - Schema ID: ${metadata.getId}, Version: ${metadata.getVersion}, Records: $recordCount")
  }

  /**
   * Emits an OpenLineage COMPLETE event with final schema from Schema Registry.
   */
  def emitCompleteEvent(
    runId: UUID,
    namespace: String,
    jobName: String,
    openlineageFileLocation: String,
    schemaRegistryUrl: String = null,
    topic: String = null,
    marquezUrl: String = "http://localhost:5050"
  ): Unit = {
    val openLineage = new OpenLineage(URI.create(producer))

    // Optionally include schema in COMPLETE event
    val inputs = if (schemaRegistryUrl != null && topic != null) {
      if (schemaRegistryClient == null) {
        schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 100)
      }
      val subject = s"$topic-value"
      val metadata = schemaRegistryClient.getLatestSchemaMetadata(subject)

      val schemaFields = new java.util.ArrayList[OpenLineage.SchemaDatasetFacetFields]()
      val avroSchema = new org.apache.avro.Schema.Parser().parse(metadata.getSchema)
      addSchemaFields(openLineage, schemaFields, avroSchema, "")

      val schemaFacet = openLineage.newSchemaDatasetFacetBuilder()
        .fields(schemaFields)
        .build()

      val inputDataset = openLineage.newInputDatasetBuilder()
        .namespace(s"kafka://localhost:9092")
        .name(topic)
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
}
