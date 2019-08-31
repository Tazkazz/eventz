package lt.tazkazz.eventz;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.msemys.esjc.*;
import com.github.msemys.esjc.system.SystemConsumerStrategy;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletionException;

import static java.util.stream.Collectors.toList;
import static lt.tazkazz.eventz.Utilz.*;

/**
 * Eventz EventStore interface
 */
public class EventzStore {
    private static final String SUBSCRIPTION_STREAM_PREFIX = "$ce-";

    private final EventStore eventStore;

    /**
     * Create the Eventz event store
     * @param host EventStore host name
     * @param port EventStore port
     * @param username EventStore user
     * @param password EventStore password
     */
    public EventzStore(String host, int port, String username, String password) {
        this.eventStore = EventStoreBuilder.newBuilder()
            .maxReconnections(-1)
            .persistentSubscriptionAutoAck(false)
            .singleNodeAddress(host, port)
            .userCredentials(username, password)
            .build();
        this.eventStore.connect();
    }

    /**
     * Persistently subscribe Tzubscriber to the EventStore
     * @param entityType Tzentity entity type
     * @param groupId Persistent subscription group ID
     * @param tzubscriber Tzubscriber to subscribe
     */
    void subscribe(String entityType, String groupId, Tzubscriber tzubscriber) {
        String streamId = SUBSCRIPTION_STREAM_PREFIX + entityType;
        PersistentSubscriptionSettings settings = PersistentSubscriptionSettings.newBuilder()
            .resolveLinkTos(true)
            .startFromBeginning()
            .namedConsumerStrategy(SystemConsumerStrategy.PINNED)
            .minCheckPointCount(1)
            .build();

        try {
            PersistentSubscriptionCreateResult result = eventStore
                .createPersistentSubscription(streamId, groupId, settings)
                .join();
            if (result.status == PersistentSubscriptionCreateStatus.Failure) {
                throw new RuntimeException(f("Failed to subscribe to '%s' as '%s'", streamId, groupId));
            }
        } catch (CompletionException e) {
            if (e.getCause() != null && e.getCause().getMessage().contains("already exists")) {
                eventStore.updatePersistentSubscription(streamId, groupId, settings).join();
            } else {
                throw new RuntimeException(f("Failed to subscribe to '%s' as '%s'", streamId, groupId));
            }
        }

        eventStore.subscribeToPersistent(streamId, groupId, tzubscriber).join();
    }

    /**
     * Write events to the EventStore stream
     * @param entityType Tzentity entity type
     * @param entityId Tzentity entity ID
     * @param events Tzevent events
     * @param expectedVersion Expected version of the stream
     * @return Next expected version of the stream
     */
    long writeEvents(String entityType, UUID entityId, List<Tzevent> events, long expectedVersion) {
        String streamId = entityType + "-" + entityId;
        List<EventData> eventData = events.stream().map(e -> this.serializeEvent(e, entityType, entityId)).collect(toList());
        WriteAttemptResult result = eventStore.tryAppendToStream(streamId, expectedVersion, eventData).join();
        if (result.status != WriteStatus.Success) {
            throw new RuntimeException(f("Failed to write events to stream '%s': %s", streamId, result.status));
        }
        return result.nextExpectedVersion;
    }

    /**
     * Read events from the EventStore stream
     * @param entityType Tzentity entity type
     * @param entityId Tzentity entity ID
     * @return Tzevent events with version
     */
    TzeventsWithVersion readEvents(String entityType, UUID entityId) {
        String streamId = entityType + "-" + entityId;
        StreamEventsSlice slice = eventStore.readStreamEventsForward(streamId, 0, 4096, true).join();
        if (slice.status != SliceReadStatus.Success) {
            throw new RuntimeException(f("Failed to read events from stream '%s': %s", streamId, slice.status));
        }
        List<Tzevent> events = slice.events.stream()
            .map(this::deserializeEvent)
            .filter(Objects::nonNull)
            .collect(toList());
        return new TzeventsWithVersion(events, slice.lastEventNumber);
    }

    /**
     * Serialize Tzevent to EventData
     * @param event Tzevent event
     * @param entityType Tzentity entity type
     * @param entityId Tzentity entity ID
     * @return EventData event
     */
    private EventData serializeEvent(Tzevent event, String entityType, UUID entityId) {
        try {
            Tzmetadata metadata = new Tzmetadata(event.getClass().getCanonicalName(), entityType, entityId);
            return EventData.newBuilder()
                .type(event.getClass().getSimpleName())
                .jsonData(OBJECT_MAPPER.writeValueAsBytes(event))
                .jsonMetadata(OBJECT_MAPPER.writeValueAsBytes(metadata))
                .build();
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Deserialize ResolvedEvent to Tzevent
     * @param eventMessage ResolvedEvent event
     * @return Tzevent event
     */
    private Tzevent deserializeEvent(ResolvedEvent eventMessage) {
        try {
            RecordedEvent event = eventMessage.event;
            if (event == null) {
                return null;
            }
            Tzmetadata metadata = getEventMetadata(event);
            Class<? extends Tzevent> eventClass = (Class<? extends Tzevent>) Class.forName(metadata.eventClass);
            return OBJECT_MAPPER.readValue(event.data, eventClass);
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}