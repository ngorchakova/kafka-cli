package org.kafka.cli.action;

import org.kafka.cli.model.KafkaConnectionConfigHolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.shell.core.CommandMarker;

/**
 * @author Natalia Gorchakova
 * @since 12/22/16
 */
public abstract class AbstractKafkaCommand implements CommandMarker {

    @Autowired
    private KafkaConnectionConfigHolder configHolder;

    public String getBrokerList() {
        return configHolder.getBrokerList();
    }

    public void setBrokerList(String brokerList) {
        configHolder.setBrokerList(brokerList);
    }
}
