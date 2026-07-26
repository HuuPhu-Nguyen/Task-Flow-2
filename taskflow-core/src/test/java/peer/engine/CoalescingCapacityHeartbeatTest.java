package peer.engine;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CoalescingCapacityHeartbeatTest {
    @Test
    void boundsQueuedWorkAndPublishesOneDirtyRerun() {
        List<Runnable> commands = new ArrayList<>();
        AtomicInteger publications = new AtomicInteger();
        CoalescingCapacityHeartbeat heartbeat =
                new CoalescingCapacityHeartbeat(commands::add, publications::incrementAndGet);

        heartbeat.request();
        heartbeat.request();
        heartbeat.request();

        assertEquals(1, commands.size());
        commands.removeFirst().run();
        assertEquals(2, publications.get());

        heartbeat.request();
        assertEquals(1, commands.size());
        commands.removeFirst().run();
        assertEquals(3, publications.get());
    }
}
