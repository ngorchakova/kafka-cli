package gorchakova.kafka.cli.config;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/**
 * @author Natalia Gorchakova
 * @since 21.12.2016.
 */
@Configuration
@ComponentScan(
        {"gorchakova.kafka.util.action",
                "gorchakova.kafka.util.nice"})
public class ShellSpringConfig {


}
