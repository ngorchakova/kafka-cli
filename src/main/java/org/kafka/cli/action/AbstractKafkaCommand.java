package org.kafka.cli.action;

import kafka.admin.AdminClient;
import org.kafka.cli.model.KafkaConnectionConfigHolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.shell.core.CommandMarker;

import java.util.HashMap;
import java.util.Map;

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
    public String getZookeeper(){
        return configHolder.getZookeeperConnection();
    }

    public AdminClient getAdminClient() {
        return configHolder.getAdminClient();
    }
    public void setBrokerList(String brokerList) {
        configHolder.setBrokerList(brokerList);
    }

    public void setZookeeper(String zookeeper) {
        configHolder.setZookeeperConnection(zookeeper);
    }

    protected Map<String, Object> getConsumerProperties(String groupId){
        Map<String, Object> properties = new HashMap<>();

        properties.put("bootstrap.servers", configHolder.getBrokerList());
        properties.put("group.id", groupId);
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return properties;
    }
}
