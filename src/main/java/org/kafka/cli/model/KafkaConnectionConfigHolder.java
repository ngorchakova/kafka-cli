package org.kafka.cli.model;

import kafka.admin.AdminClient;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.security.JaasUtils;
import org.springframework.stereotype.Component;

import java.util.Properties;

/**
 * @author Natalia Gorchakova
 * @since 22.12.2016.
 */
@Component
public class KafkaConnectionConfigHolder {

    private String brokerList;
    private String zookeeperConnection;
    private AdminClient adminClient;

    public String getBrokerList() {
        return brokerList;
    }

    public void setBrokerList(String brokerList) {
        this.brokerList = brokerList;
        Properties properties = new Properties();
        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        this.adminClient = AdminClient.create(properties);
    }

    public String getZookeeperConnection() {
        return zookeeperConnection;
    }

    public AdminClient getAdminClient() {
        return adminClient;
    }

    public void setZookeeperConnection(String zookeeperConnection) {
        this.zookeeperConnection = zookeeperConnection;

    }
}
