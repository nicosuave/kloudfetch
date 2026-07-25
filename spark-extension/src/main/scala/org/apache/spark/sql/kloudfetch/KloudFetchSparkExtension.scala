/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.kloudfetch

import java.nio.charset.StandardCharsets
import java.util.{Base64, HashMap}

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.arrow.vector.types.pojo.{Field, FieldType, Schema}
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.TaskContext
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode, UnresolvedHint}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy, UnaryExecNode}
import org.apache.spark.sql.execution.arrow.ArrowConverters
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.types.StringType
import org.apache.spark.util.SerializableConfiguration
import org.apache.arrow.vector.util.SchemaUtility

/** Entry point configured through spark.sql.extensions. */
final class KloudFetchSparkExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectResolutionRule(session => new KloudFetchHintRule)
    extensions.injectPlannerStrategy(session => new KloudFetchStrategy(session))
  }
}

/** Turns the private KLOUDFETCH optimizer hint into an explicit unary plan. */
private final class KloudFetchHintRule extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformUp {
    case UnresolvedHint(name, Seq(Literal(value, StringType)), child)
        if name.equalsIgnoreCase("KLOUDFETCH") =>
      val id = String.valueOf(value)
      require(id.matches("[a-fA-F0-9]{32}"), s"invalid KloudFetch query id: $id")
      KloudFetchLogicalPlan(id.toLowerCase, child)
  }
}

private final case class KloudFetchLogicalPlan(
    queryId: String,
    child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
    copy(child = newChild)
}

private final class KloudFetchStrategy(session: SparkSession) extends SparkStrategy {
  private val minimumEstimatedBytes =
    session.conf.get("spark.kloudfetch.minEstimatedBytes", "0").toLong
  private val bucket = session.conf.get("spark.kloudfetch.bucket")
  private val prefix = session.conf.get("spark.kloudfetch.prefix", "results").stripSuffix("/")
  private val maxRecordsPerBatch =
    session.conf.get("spark.kloudfetch.maxRecordsPerBatch", "250000").toLong
  private val maxEstimatedBatchBytes =
    session.conf.get("spark.kloudfetch.maxEstimatedBatchBytes", "67108864").toLong
  private val timeZoneId = session.sessionState.conf.sessionLocalTimeZone
  private val failFirstAttemptPartition =
    session.conf.get("spark.kloudfetch.test.failFirstAttemptPartition", "-1").toInt
  private val slowFirstAttemptPartition =
    session.conf.get("spark.kloudfetch.test.slowFirstAttemptPartition", "-1").toInt
  private val slowFirstAttemptMillis =
    session.conf.get("spark.kloudfetch.test.slowFirstAttemptMillis", "0").toLong
  private val failDuringUploadPartition =
    session.conf.get("spark.kloudfetch.test.failDuringUploadPartition", "-1").toInt
  private val hadoopConfiguration =
    new SerializableConfiguration(session.sparkContext.hadoopConfiguration)

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case KloudFetchLogicalPlan(queryId, child)
        if child.stats.sizeInBytes >= BigInt(minimumEstimatedBytes) =>
      KloudFetchExec(
        queryId,
        bucket,
        prefix,
        maxRecordsPerBatch,
        maxEstimatedBatchBytes,
        timeZoneId,
        failFirstAttemptPartition,
        slowFirstAttemptPartition,
        slowFirstAttemptMillis,
        failDuringUploadPartition,
        hadoopConfiguration,
        planLater(child)) :: Nil
    case KloudFetchLogicalPlan(_, child) =>
      planLater(child) :: Nil
    case _ => Nil
  }
}

private final case class KloudFetchExec(
    queryId: String,
    bucket: String,
    prefix: String,
    maxRecordsPerBatch: Long,
    maxEstimatedBatchBytes: Long,
    timeZoneId: String,
    failFirstAttemptPartition: Int,
    slowFirstAttemptPartition: Int,
    slowFirstAttemptMillis: Long,
    failDuringUploadPartition: Int,
    hadoopConfiguration: SerializableConfiguration,
    child: SparkPlan) extends UnaryExecNode {

  override def output: Seq[Attribute] = child.output

  override protected def doExecute(): RDD[InternalRow] = {
    val schema = child.schema
    val baseArrowSchema = ArrowUtils.toArrowSchema(
      schema,
      timeZoneId,
      errorOnDuplicatedFieldNames = false,
      largeVarTypes = false)
    val enrichedFields = baseArrowSchema.getFields.asScala
      .zip(schema.fields)
      .map { case (arrowField, sparkField) =>
        val fieldType = arrowField.getFieldType
        val metadata = new HashMap[String, String](arrowField.getMetadata)
        metadata.put("Spark:DataType:SqlName", sparkField.dataType.sql)
        new Field(
          arrowField.getName,
          new FieldType(
            fieldType.isNullable,
            fieldType.getType,
            fieldType.getDictionary,
            metadata),
          arrowField.getChildren)
      }
      .asJava
    val serializedArrowSchema = Base64.getEncoder.encodeToString(
      SchemaUtility.serialize(
        new Schema(enrichedFields, baseArrowSchema.getCustomMetadata)))
    child.execute().mapPartitionsWithIndex { (partitionId, rows) =>
      val attemptNumber = TaskContext.get().attemptNumber()
      if (partitionId == slowFirstAttemptPartition &&
          attemptNumber == 0 && slowFirstAttemptMillis > 0) {
        Thread.sleep(slowFirstAttemptMillis)
      }
      val iterator = ArrowConverters.toBatchWithSchemaIterator(
        rows,
        schema,
        maxRecordsPerBatch,
        maxEstimatedBatchBytes,
        timeZoneId,
        errorOnDuplicatedFieldNames = false,
        largeVarTypes = false)
      var batchId = 0
      try {
        while (iterator.hasNext) {
          val arrow = iterator.next()
          val rowCount = iterator.rowCountInLastBatch
          val base = f"$prefix/$queryId/part-$partitionId%05d-batch-$batchId%05d"
          val arrowKey = s"$base.arrow"
          val arrowPath = new Path(s"s3a://$bucket/$arrowKey")
          val fileSystem = arrowPath.getFileSystem(hadoopConfiguration.value)
          val output = fileSystem.create(arrowPath, true)
          try {
            if (partitionId == failDuringUploadPartition &&
                attemptNumber == 0 && batchId == 0) {
              output.write(arrow, 0, Math.max(1, arrow.length / 2))
              throw new IllegalStateException(
                s"injected partial upload for partition $partitionId")
            } else {
              output.write(arrow)
            }
          } finally {
            output.close()
          }

          val sidecar =
            s"""{"key":"$arrowKey","rows":$rowCount,"bytes":${arrow.length},""" +
              s""""partition":$partitionId,"batch":$batchId,""" +
              s""""arrow_schema":"$serializedArrowSchema"}"""
          val sidecarPath = new Path(s"s3a://$bucket/$base.json")
          val sidecarOutput = fileSystem.create(sidecarPath, true)
          try {
            sidecarOutput.write(sidecar.getBytes(StandardCharsets.UTF_8))
          } finally {
            sidecarOutput.close()
          }
          batchId += 1
          if (partitionId == failFirstAttemptPartition &&
              attemptNumber == 0 && batchId == 1) {
            throw new IllegalStateException(
              s"injected first-attempt failure for partition $partitionId")
          }
        }
      } catch {
        case NonFatal(error) =>
          throw new IllegalStateException(
            s"failed to spool KloudFetch partition $partitionId for $queryId",
            error)
      }
      Iterator.empty
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}
