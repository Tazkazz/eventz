package lt.tazkazz.eventz;

import java.util.UUID;

/**
 * Tzentity with it's ID and version
 * @param <T> Tzentity entity
 */
public class TzentityWithIdAndVersion<T extends Tzentity> {
    private final UUID id;
    private final long version;
    private final T entity;

    TzentityWithIdAndVersion(UUID id, long version, T entity) {
        this.id = id;
        this.version = version;
        this.entity = entity;
    }

    public UUID getId() {
        return id;
    }

    public long getVersion() {
        return version;
    }

    public T getEntity() {
        return entity;
    }
}
