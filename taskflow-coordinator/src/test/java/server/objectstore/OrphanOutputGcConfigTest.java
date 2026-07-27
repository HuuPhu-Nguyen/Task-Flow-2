package server.objectstore;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OrphanOutputGcConfigTest {
    @Test
    void defaultsAreConservativeAndBounded() {
        OrphanOutputGcConfig config =
                OrphanOutputGcConfig.fromSources(Map.of(), new Properties());

        assertTrue(config.enabled());
        assertEquals(24L * 60L * 60L * 1_000L, config.safetyWindowMillis());
        assertEquals(5L * 60L * 1_000L, config.intervalMillis());
        assertEquals(100, config.batchSize());
    }

    @Test
    void systemPropertiesOverrideEnvironment() {
        Properties properties = new Properties();
        properties.setProperty(OrphanOutputGcConfig.ENABLED_PROPERTY, "false");
        properties.setProperty(OrphanOutputGcConfig.SAFETY_WINDOW_PROPERTY, "2000");
        properties.setProperty(OrphanOutputGcConfig.INTERVAL_PROPERTY, "3000");
        properties.setProperty(OrphanOutputGcConfig.BATCH_SIZE_PROPERTY, "4");

        OrphanOutputGcConfig config = OrphanOutputGcConfig.fromSources(
                Map.of(
                        OrphanOutputGcConfig.ENABLED_ENV, "true",
                        OrphanOutputGcConfig.SAFETY_WINDOW_ENV, "9000",
                        OrphanOutputGcConfig.INTERVAL_ENV, "9000",
                        OrphanOutputGcConfig.BATCH_SIZE_ENV, "10"
                ),
                properties
        );

        assertFalse(config.enabled());
        assertEquals(2_000L, config.safetyWindowMillis());
        assertEquals(3_000L, config.intervalMillis());
        assertEquals(4, config.batchSize());
    }

    @Test
    void invalidOrUnboundedValuesAreRejected() {
        assertThrows(IllegalArgumentException.class, () ->
                OrphanOutputGcConfig.fromSources(
                        Map.of(OrphanOutputGcConfig.ENABLED_ENV, "sometimes"),
                        new Properties()
                ));
        assertThrows(IllegalArgumentException.class, () ->
                new OrphanOutputGcConfig(true, 0L, 1L, 2));
        assertThrows(IllegalArgumentException.class, () ->
                new OrphanOutputGcConfig(true, 1L, 0L, 2));
        assertThrows(IllegalArgumentException.class, () ->
                new OrphanOutputGcConfig(true, 1L, 1L, 1));
        assertThrows(IllegalArgumentException.class, () ->
                new OrphanOutputGcConfig(true, 1L, 1L, 1_001));
    }
}
