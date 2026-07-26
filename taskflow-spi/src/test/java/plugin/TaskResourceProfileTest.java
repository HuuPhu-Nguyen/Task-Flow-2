package plugin;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TaskResourceProfileTest {
    @Test
    void capacityOnlyFactoryUsesAbsentDiagnostics() {
        TaskResourceProfile profile = TaskResourceProfile.ofCapacityUnits(1);

        assertEquals(1, profile.capacityUnitCost());
        assertFalse(profile.estimatedMemoryBytes().isPresent());
        assertFalse(profile.estimatedTemporaryDiskBytes().isPresent());
    }

    @Test
    void rejectsInvalidCostsAndPresentDiagnosticBounds() {
        assertThrows(
                IllegalArgumentException.class,
                () -> TaskResourceProfile.ofCapacityUnits(0)
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> new TaskResourceProfile(1, OptionalLong.of(0L), OptionalLong.empty())
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> new TaskResourceProfile(1, OptionalLong.empty(), OptionalLong.of(-1L))
        );
    }

    @Test
    void catalogNormalizesAndDefensivelyCapturesProfiles() {
        TaskResourceCatalog catalog = TaskResourceCatalog.capture(Map.of(
                " text_analysis ",
                TaskResourceProfile.ofCapacityUnits(1)
        ));

        assertEquals(1, catalog.require("TEXT_ANALYSIS").capacityUnitCost());
        assertThrows(
                IllegalArgumentException.class,
                () -> catalog.require("VIDEO_TRANSCODING")
        );
        assertThrows(
                UnsupportedOperationException.class,
                () -> catalog.asMap().clear()
        );
    }
}
