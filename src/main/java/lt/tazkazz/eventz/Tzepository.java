package lt.tazkazz.eventz;

import com.github.msemys.esjc.ExpectedVersion;

import java.util.List;
import java.util.UUID;

/**
 * Tzentity repository for operations with entities
 * @param <T> Tzentity entity
 * @param <C> Tzcommand base entity command
 */
public class Tzepository<T extends Tzentity<T, C>, C extends Tzcommand> {
    private final Class<T> entityClass;
    private final String entityType;
    private final EventzStore eventzStore;

    public Tzepository(Class<T> entityClass, EventzStore eventzStore) {
        this.entityClass = entityClass;
        this.entityType = entityClass.getSimpleName();
        this.eventzStore = eventzStore;
    }

    /**
     * Save a new entity to the EventStore
     * @param command Tzcommand command
     * @return Tzentity with ID and new version
     */
    public TzentityWithIdAndVersion<T> save(C command) {
        T entity = newEntity();
        UUID uuid = UUID.randomUUID();
        List<Tzevent> events = entity.processCommand(command);
        long newVersion = writeEvents(events, uuid, ExpectedVersion.NO_STREAM);
        events.forEach(entity::applyEvent);
        return new TzentityWithIdAndVersion<>(uuid, newVersion, entity);
    }

    /**
     * Update an existing entity in the EventStore
     * @param entityId Tzentity entity ID
     * @param command Tzcommand command
     * @return Tzentity with ID and new version
     */
    public TzentityWithIdAndVersion<T> update(UUID entityId, C command) {
        TzentityWithIdAndVersion<T> entityWithIdAndVersion = loadEntity(entityId);
        T entity = entityWithIdAndVersion.getEntity();
        List<Tzevent> events = entity.processCommand(command);
        long newVersion = writeEvents(events, entityId, entityWithIdAndVersion.getVersion());
        events.forEach(entity::applyEvent);
        return new TzentityWithIdAndVersion<>(entityId, newVersion, entity);
    }

    /**
     * Write Tzevent events to the EventStore for Tzentity
     * @param events Tzevent events
     * @param entityId Tzentity entity ID
     * @param expectedVersion Expected version of the entity
     * @return Next expected version of the stream
     */
    private long writeEvents(List<Tzevent> events, UUID entityId, long expectedVersion) {
        return eventzStore.writeEvents(entityType, entityId, events, expectedVersion);
    }

    /**
     * Create an new Tzentity entity
     * @return Tzentity entity
     */
    private T newEntity() {
        try {
            return entityClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Load the most recent Tzentity entity
     * @param entityId Tzentity entity ID
     * @return Tzentity entity with ID and version
     */
    private TzentityWithIdAndVersion<T> loadEntity(UUID entityId) {
        T entity = newEntity();
        TzeventsWithVersion eventsWithVersion = eventzStore.readEvents(entityType, entityId);
        eventsWithVersion.getEvents().forEach(entity::applyEvent);
        return new TzentityWithIdAndVersion<>(entityId, eventsWithVersion.getVersion(), entity);
    }
}
