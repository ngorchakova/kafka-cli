package org.kafka.cli.utils

import java.nio.ByteBuffer

import org.apache.kafka.common.protocol.types.Type.{INT32, STRING}
import org.apache.kafka.common.protocol.types.{Field, Schema, Struct}
import org.apache.kafka.common.utils.Utils

/**
  * part of `kafka.coordinator.GroupMetadataManager`
  *
  * @author menshin on 3/7/17.
  */
object KafkaOffsetTopicManager {

  val ConsumerOffsetTopic = "__consumer_offsets"

  private val CURRENT_OFFSET_KEY_SCHEMA_VERSION = 1.toShort
  private val OFFSET_COMMIT_KEY_SCHEMA = new Schema(new Field("group", STRING),
    new Field("topic", STRING),
    new Field("partition", INT32))

  private val OFFSET_KEY_GROUP_FIELD = OFFSET_COMMIT_KEY_SCHEMA.get("group")
  private val OFFSET_KEY_TOPIC_FIELD = OFFSET_COMMIT_KEY_SCHEMA.get("topic")
  private val OFFSET_KEY_PARTITION_FIELD = OFFSET_COMMIT_KEY_SCHEMA.get("partition")


  /**
    * Generates the key for offset commit message for given (group, topic, partition)
    * copy of kafka.coordinator.GroupMetadataManager#offsetCommitKey
    * @return key for offset commit message
    */
  def offsetCommitKey(group: String, topic: String, partition: Int, versionId: Short = 0): Array[Byte] = {
    val key = new Struct(OFFSET_COMMIT_KEY_SCHEMA)
    key.set(OFFSET_KEY_GROUP_FIELD, group)
    key.set(OFFSET_KEY_TOPIC_FIELD, topic)
    key.set(OFFSET_KEY_PARTITION_FIELD, partition)

    val byteBuffer = ByteBuffer.allocate(2 /* version */ + key.sizeOf)
    byteBuffer.putShort(CURRENT_OFFSET_KEY_SCHEMA_VERSION)
    key.writeTo(byteBuffer)
    byteBuffer.array()
  }


  def partitionFor(groupId: String, offsetTopicPartitionCount: Int): Int = Utils.abs(groupId.hashCode) % offsetTopicPartitionCount
}
