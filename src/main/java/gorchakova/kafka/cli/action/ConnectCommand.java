package gorchakova.kafka.cli.action;

import gorchakova.kafka.cli.model.KafkaConnectionConfigHolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

/**
 * @author Natalia Gorchakova
 * @since 21.12.2016.
 */
@Component
public class ConnectCommand implements CommandMarker {

    @Autowired
    private KafkaConnectionConfigHolder configHolder;

    @CliAvailabilityIndicator({"connect"})
    public boolean isConnectAvailable() {
        return true;
    }


    @CliCommand(value = "connect", help = "Set broker list for future operations")
    public String simple(
            @CliOption(key = {"brokerList"}, mandatory = true, help = "brokerList") final String brokerList) {
        configHolder.setBrokerList(brokerList);
        return "broker list was changed to " + brokerList;
    }
}
