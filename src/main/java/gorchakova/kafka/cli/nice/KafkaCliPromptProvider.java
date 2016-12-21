package gorchakova.kafka.cli.nice;

import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.shell.plugin.support.DefaultPromptProvider;
import org.springframework.stereotype.Component;

/**
 * @author Natalia Gorchakova
 * @since 22.12.2016.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class KafkaCliPromptProvider extends DefaultPromptProvider {

    @Override
    public String getPrompt() {
        return "kafka-shell>";
    }

}
