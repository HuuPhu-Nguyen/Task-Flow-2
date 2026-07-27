package server.metrics;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MetricsEndpointConfigTest {
    @Test
    void defaultsAreEnabledAndLoopbackOnly() {
        MetricsEndpointConfig config =
                MetricsEndpointConfig.fromSources(Map.of(), new Properties());

        assertTrue(config.enabled());
        assertEquals("127.0.0.1", config.host());
        assertEquals(9464, config.port());
    }

    @Test
    void systemPropertiesOverrideEnvironment() {
        Properties properties = new Properties();
        properties.setProperty(MetricsEndpointConfig.ENABLED_PROPERTY, "false");
        properties.setProperty(MetricsEndpointConfig.HOST_PROPERTY, "localhost");
        properties.setProperty(MetricsEndpointConfig.PORT_PROPERTY, "19464");

        MetricsEndpointConfig config = MetricsEndpointConfig.fromSources(
                Map.of(
                        MetricsEndpointConfig.ENABLED_ENV, "true",
                        MetricsEndpointConfig.HOST_ENV, "0.0.0.0",
                        MetricsEndpointConfig.PORT_ENV, "9464"
                ),
                properties
        );

        assertFalse(config.enabled());
        assertEquals("localhost", config.host());
        assertEquals(19464, config.port());
    }

    @Test
    void rejectsInvalidBooleanHostAndPort() {
        assertThrows(
                IllegalArgumentException.class,
                () -> MetricsEndpointConfig.fromSources(
                        Map.of(MetricsEndpointConfig.ENABLED_ENV, "sometimes"),
                        new Properties()
                )
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> new MetricsEndpointConfig(true, "bad host", 9464)
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> new MetricsEndpointConfig(true, "127.0.0.1", 65_536)
        );
    }
}
