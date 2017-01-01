package org.kafka.cli.action;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * @author Natalia Gorchakova
 * @since 12/22/16
 */
@Component
public class GetOffsetsCommand extends AbstractKafkaCommand {

    private final static String GROUP_ID = "GetOffsetShell";

    @CliAvailabilityIndicator({"getOffsets"})
    public boolean isAvailable() {
        return !StringUtils.isEmpty(getBrokerList());
    }


    @CliCommand(value = "getOffsets", help = "Get first/last offets for topic")
    public String simple(
            @CliOption(key = {"topic"}, mandatory = true, help = "topic name") final String topic) {


        try (KafkaConsumer<Object, Object> consumer = new KafkaConsumer<>(getConsumerProperties(GROUP_ID))) {
            Map<Integer, OffsetRange> topicOffsets = consumer.partitionsFor(topic).stream().collect(Collectors.toMap(PartitionInfo::partition,
                    pi -> {
                        TopicPartition topicPartition = new TopicPartition(pi.topic(), pi.partition());
                        consumer.assign(Collections.singletonList(topicPartition));
                        consumer.seekToBeginning(topicPartition);
                        long firstOffset = consumer.position(topicPartition);

                        consumer.seekToEnd(topicPartition);
                        long lastOffset = consumer.position(topicPartition);

                        return new OffsetRange(firstOffset, lastOffset);
                    }, (u,v) -> { throw new IllegalStateException(String.format("Duplicate key %s", u)); }, TreeMap::new)
                    );
            consumer.unsubscribe();
            return topicOffsets.entrySet()
                    .stream()
                    .map(entry -> String.format("%d: %d - %d", entry.getKey(), entry.getValue().getFirstOffset(), entry.getValue().getLastOffset()))
                    .collect(Collectors.joining("\n"));
        }
    }

    private class OffsetRange{
        private final Long firstOffset;
        private final Long lastOffset;

        private OffsetRange(Long firstOffset, Long lastOffset) {
            this.firstOffset = firstOffset;
            this.lastOffset = lastOffset;
        }

        Long getFirstOffset() {
            return firstOffset;
        }

        Long getLastOffset() {
            return lastOffset;
        }

    }
}
