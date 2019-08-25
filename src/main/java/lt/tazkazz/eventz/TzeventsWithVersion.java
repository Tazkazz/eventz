package lt.tazkazz.eventz;

import java.util.List;

/**
 * Tzevent events with version
 */
class TzeventsWithVersion {
    private final List<Tzevent> events;
    private final long version;

    TzeventsWithVersion(List<Tzevent> events, long version) {
        this.events = events;
        this.version = version;
    }

    List<Tzevent> getEvents() {
        return events;
    }

    long getVersion() {
        return version;
    }
}
