package gui;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import static org.junit.jupiter.api.Assertions.assertEquals;

class GuiHistoryViewTest {
    @Test
    void abbreviatesLongIdsForTables() {
        assertEquals("", GuiHistoryView.abbreviateId(null));
        assertEquals("short-id", GuiHistoryView.abbreviateId("short-id"));
        assertEquals("abcdefghijkl...", GuiHistoryView.abbreviateId("abcdefghijklmnop"));
    }

    @Test
    void formatsDurationOnlyWhenStartAndCompletionAreKnown() {
        assertEquals("-", GuiHistoryView.formatDurationSeconds(0, 2000));
        assertEquals("-", GuiHistoryView.formatDurationSeconds(1000, 0));
        assertEquals("1.5 s", GuiHistoryView.formatDurationSeconds(1000, 2500));
    }

    @Test
    void formatsMissingTimestampAsDash() {
        assertEquals("-", GuiHistoryView.formatTimestamp(0));
    }

    @Test
    void formatsEpochMillisWithSystemZone() {
        long epochMillis = 1500L;
        String expected = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                .withZone(ZoneId.systemDefault())
                .format(Instant.ofEpochMilli(epochMillis));

        assertEquals(expected, GuiHistoryView.formatTimestamp(epochMillis));
    }
}
