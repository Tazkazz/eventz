package lt.tazkazz.eventz;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.msemys.esjc.*;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.ForOverride;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.util.Arrays;
import java.util.Set;
import java.util.UUID;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static com.fasterxml.jackson.databind.MapperFeature.DEFAULT_VIEW_INCLUSION;
import static lt.tazkazz.eventz.Utilz.invokeMethod;

/**
 * Tzubscriber persistent subscriber for the EventStore
 */
public abstract class Tzubscriber implements PersistentSubscriptionListener {
    private static final Set<SubscriptionDropReason> RECOVERABLE_SUBSCRIPTION_DROP_REASONS = ImmutableSet.of(
        SubscriptionDropReason.ConnectionClosed,
        SubscriptionDropReason.ServerError,
        SubscriptionDropReason.SubscribingError,
        SubscriptionDropReason.CatchUpError
    );

    private final EventzStore eventzStore;
    private final String entityType;
    private final String groupId;
    private final ObjectMapper objectMapper;

    public Tzubscriber(EventzStore eventzStore, String entityType, String groupId) {
        this.eventzStore = eventzStore;
        this.entityType = entityType;
        this.groupId = groupId;
        this.objectMapper = new ObjectMapper()
            .configure(DEFAULT_VIEW_INCLUSION, false)
            .configure(FAIL_ON_UNKNOWN_PROPERTIES, false);
        subscribe();
    }

    /**
     * Subscribe to the EventStore
     */
    private void subscribe() {
        eventzStore.subscribe("$ce-" + entityType, groupId, this);
    }

    @Override
    public final void onEvent(PersistentSubscription subscription, RetryableResolvedEvent eventMessage) {
        RecordedEvent event = eventMessage.event;
        if (event == null) {
            subscription.acknowledge(eventMessage);
            return;
        }
        Class<? extends Tzevent> eventClass = getEventClass(event.eventType);
        if (eventClass != null && event.eventStreamId.startsWith(entityType)) {
            handleEvent(event, eventClass);
        } else {
            logUnknown(event);
        }
        subscription.acknowledge(eventMessage);
    }

    @Override
    public final void onClose(PersistentSubscription subscription, SubscriptionDropReason reason, Exception exception) {
        if (exception instanceof EventStoreException || RECOVERABLE_SUBSCRIPTION_DROP_REASONS.contains(reason)) {
            subscribe();
            return;
        }
        throw new RuntimeException(exception);
    }

    /**
     * Log event on handle
     * @param envelope Tzenvelope envelope
     */
    @ForOverride
    public void logHandlingEvent(Tzenvelope envelope) {}

    /**
     * Log event on ignore
     * @param event RecordedEvent event
     */
    @ForOverride
    public void logUnknown(RecordedEvent event) {}

    /**
     * Handle event
     * @param event RecordedEvent event
     * @param eventClass Tzevent event class
     */
    private void handleEvent(RecordedEvent event, Class<? extends Tzevent> eventClass) {
        try {
            Tzevent tzevent = objectMapper.readValue(event.data, eventClass);
            Tzenvelope tzenvelope = new Tzenvelope<>(entityType, parseEntityId(event.eventStreamId), tzevent);
            logHandlingEvent(tzenvelope);
            Arrays.stream(this.getClass().getDeclaredMethods())
                .filter(method -> isEventForThisMethod(method, eventClass))
                .forEach(method -> invokeMethod(this, method, tzenvelope));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get Tzevent event class by the type
     * @param eventType Tzevent event type
     * @return Tzevent event class
     */
    private static Class<? extends Tzevent> getEventClass(String eventType) {
        try {
            return (Class<? extends Tzevent>) Class.forName(eventType);
        } catch (ClassNotFoundException e) {
            return null;
        }
    }

    /**
     * Check if given Tzubscriber method is for given Tzevent event
     * @param method Tzubscriber method
     * @param eventClass Tzevent event class
     * @return Is the method for given Tzevent event
     */
    private boolean isEventForThisMethod(Method method, Class<? extends Tzevent> eventClass) {
        return method.getAnnotation(Tzhandler.class) != null &&
            method.getParameterCount() == 1 &&
            ((ParameterizedType) method.getGenericParameterTypes()[0]).getActualTypeArguments()[0] == eventClass;
    }

    /**
     * Parse Tzentity entity ID from EventStore event stream ID
     * @param eventStreamId EventStore event stream ID
     * @return Tzentity entity ID
     */
    private UUID parseEntityId(String eventStreamId) {
        String entityId = eventStreamId.substring(entityType.length() + 1);
        return UUID.fromString(entityId);
    }
}
