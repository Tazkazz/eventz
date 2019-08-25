package lt.tazkazz.eventz;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.msemys.esjc.*;
import com.github.msemys.esjc.system.SystemConsumerStrategy;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionException;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static com.fasterxml.jackson.databind.MapperFeature.DEFAULT_VIEW_INCLUSION;
import static java.util.stream.Collectors.toList;
import static lt.tazkazz.eventz.Utilz.f;

/**
 * Eventz EventStore interface
 */
public class EventzStore {
    private final EventStore eventStore;
    private final ObjectMapper objectMapper;

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
        this.objectMapper = new ObjectMapper()
            .configure(DEFAULT_VIEW_INCLUSION, false)
            .configure(FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    /**
     * Persistently subscribe Tzubscriber to the EventStore
     * @param streamId EventStore stream ID
     * @param groupId Persistent subscription group ID
     * @param tzubscriber Tzubscriber to subscribe
     */
    void subscribe(String streamId, String groupId, Tzubscriber tzubscriber) {
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
     * @param streamId EventStore stream ID
     * @param events Tzevent events
     * @param expectedVersion Expected version of the stream
     * @return Next expected version of the stream
     */
    long writeEvents(String streamId, List<Tzevent> events, long expectedVersion) {
        List<EventData> eventData = events.stream().map(this::serializeEvent).collect(toList());
        WriteAttemptResult result = eventStore.tryAppendToStream(streamId, expectedVersion, eventData).join();
        if (result.status != WriteStatus.Success) {
            throw new RuntimeException(f("Failed to write events to stream '%s': %s", streamId, result.status));
        }
        return result.nextExpectedVersion;
    }

    /**
     * Read events from the EventStore stream
     * @param streamId EventStore stream ID
     * @return Tzevent events with version
     */
    TzeventsWithVersion readEvents(String streamId) {
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
     * @return EventData event
     */
    private EventData serializeEvent(Tzevent event) {
        try {
            return EventData.newBuilder()
                .type(event.getClass().getCanonicalName())
                .jsonData(objectMapper.writeValueAsBytes(event))
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
            Class<? extends Tzevent> eventClass = (Class<? extends Tzevent>) Class.forName(event.eventType);
            return objectMapper.readValue(event.data, eventClass);
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}