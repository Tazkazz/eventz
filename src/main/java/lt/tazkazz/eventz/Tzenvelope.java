package lt.tazkazz.eventz;

import java.util.UUID;

/**
 * Tzevent envelope with entity type and ID
 * @param <T> Tzevent event
 */
public class Tzenvelope<T extends Tzevent> {
    private final String entityType;
    private final UUID entityId;
    private final T event;

    Tzenvelope(String entityType, UUID entityId, T event) {
        this.entityType = entityType;
        this.entityId = entityId;
        this.event = event;
    }

    public String getEntityType() {
        return entityType;
    }

    public UUID getEntityId() {
        return entityId;
    }

    public T getEvent() {
        return event;
    }
}
