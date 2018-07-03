package org.kafka.cli.utils

import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig => KafkaConsumerConfig}

/**
  * @author menshin on 7/2/18.
  */
case class ConsumerConfig(brokers: String, groupId: String, additionalConfig: Option[String])

object ConsumerConfig{

  def createConsumerConfig(config: ConsumerConfig): Properties = {
    val props = new Properties()
    props.put(KafkaConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.brokers)
    props.put(KafkaConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(KafkaConsumerConfig.GROUP_ID_CONFIG, config.groupId)
    props.put(KafkaConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(KafkaConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    config.additionalConfig.foreach(path => props.putAll(PropertyUtils.loadProps(path)))

    props
  }


}
