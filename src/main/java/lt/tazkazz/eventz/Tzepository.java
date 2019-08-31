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
        return processCommand(newEntityWithIdAndVersion(), command);
    }

    /**
     * Update an existing entity in the EventStore
     * @param entityId Tzentity entity ID
     * @param command Tzcommand command
     * @return Tzentity with ID and new version
     */
    public TzentityWithIdAndVersion<T> update(UUID entityId, C command) {
        return processCommand(loadEntity(entityId), command);
    }

    /**
     * Process Tzcommand command and write Tzevent events
     * @param entityWithIdAndVersion Tzentity entity with ID and version
     * @param command Tzcommand command
     * @return Updated Tzentity entity with ID and version
     */
    private TzentityWithIdAndVersion<T> processCommand(TzentityWithIdAndVersion<T> entityWithIdAndVersion, C command) {
        T entity = entityWithIdAndVersion.getEntity();
        UUID id = entityWithIdAndVersion.getId();
        List<Tzevent> events = entity.processCommand(command);
        long newVersion = writeEvents(events, id, entityWithIdAndVersion.getVersion());
        events.forEach(entity::applyEvent);
        return new TzentityWithIdAndVersion<>(id, newVersion, entity);
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
     * Create a new Tzentity entity
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
     * Create a new Tzentity entity with ID and version
     * @return Tzentity entity with ID and version
     */
    private TzentityWithIdAndVersion<T> newEntityWithIdAndVersion() {
        return new TzentityWithIdAndVersion<>(UUID.randomUUID(), ExpectedVersion.NO_STREAM, newEntity());
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
