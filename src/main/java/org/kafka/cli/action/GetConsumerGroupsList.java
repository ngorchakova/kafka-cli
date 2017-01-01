package org.kafka.cli.action;

import kafka.coordinator.GroupOverview;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Natalia Gorchakova
 * @since 25.12.2016
 */
@Component
public class GetConsumerGroupsList extends AbstractKafkaCommand {

    @CliAvailabilityIndicator({"getConsumersList"})
    public boolean isAvailable() {
        return !StringUtils.isEmpty(getZookeeper());
    }


    @CliCommand(value = "getConsumersList", help = "Get list of all consumer groups")
    public String action(
            @CliOption(key = {"old"}, specifiedDefaultValue = "true", unspecifiedDefaultValue = "false", mandatory = false, help = "old groups for kafka 8 high level consumer") final boolean old) {
        if (old) {
            throw new UnsupportedOperationException();
        } else {
            List<GroupOverview> groups = new ArrayList<>();
            getAdminClient().listAllConsumerGroupsFlattened().foreach(groups::add);
            return groups.stream().map(GroupOverview::groupId).collect(Collectors.joining("\n"));
        }
    }

}
