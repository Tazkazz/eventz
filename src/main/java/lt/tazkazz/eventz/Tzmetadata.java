package lt.tazkazz.eventz;

import java.util.UUID;

/**
 * Tzevent metadata
 */
public class Tzmetadata {
    public String eventClass;
    public String entityType;
    public UUID entityId;

    public Tzmetadata() {}

    public Tzmetadata(String eventClass, String entityType, UUID entityId) {
        this.eventClass = eventClass;
        this.entityType = entityType;
        this.entityId = entityId;
    }
}
