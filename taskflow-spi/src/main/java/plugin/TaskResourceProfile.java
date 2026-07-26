package plugin;

import java.util.OptionalLong;

/**
 * Immutable scalar scheduling cost plus diagnostic-only resource estimates.
 */
public record TaskResourceProfile(
        int capacityUnitCost,
        OptionalLong estimatedMemoryBytes,
        OptionalLong estimatedTemporaryDiskBytes
) {
    public TaskResourceProfile {
        if (capacityUnitCost <= 0) {
            throw new IllegalArgumentException("capacityUnitCost must be positive.");
        }
        estimatedMemoryBytes = requirePositiveIfPresent(
                estimatedMemoryBytes,
                "estimatedMemoryBytes"
        );
        estimatedTemporaryDiskBytes = requirePositiveIfPresent(
                estimatedTemporaryDiskBytes,
                "estimatedTemporaryDiskBytes"
        );
    }

    public static TaskResourceProfile ofCapacityUnits(int capacityUnitCost) {
        return new TaskResourceProfile(
                capacityUnitCost,
                OptionalLong.empty(),
                OptionalLong.empty()
        );
    }

    private static OptionalLong requirePositiveIfPresent(OptionalLong value, String field) {
        OptionalLong checked = value == null ? OptionalLong.empty() : value;
        if (checked.isPresent() && checked.getAsLong() <= 0L) {
            throw new IllegalArgumentException(field + " must be positive when present.");
        }
        return checked;
    }
}
