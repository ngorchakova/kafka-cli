package gorchakova.kafka.shell.model;

import org.springframework.stereotype.Component;

/**
 * @author Natalia Gorchakova
 * @since 22.12.2016.
 */
@Component
public class KafkaConnectionConfigHolder {

    private String brokerList;

    public String getBrokerList() {
        return brokerList;
    }

    public void setBrokerList(String brokerList) {
        this.brokerList = brokerList;
    }
}
