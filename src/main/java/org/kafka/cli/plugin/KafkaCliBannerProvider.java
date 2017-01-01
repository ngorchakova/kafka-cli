package org.kafka.cli.plugin;

import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.shell.plugin.support.DefaultBannerProvider;
import org.springframework.stereotype.Component;

/**
 * @author Natalia Gorchakova
 * @since 21.12.2016.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class KafkaCliBannerProvider extends DefaultBannerProvider {


}
