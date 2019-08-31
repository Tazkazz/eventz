package lt.tazkazz.eventz;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.msemys.esjc.RecordedEvent;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static com.fasterxml.jackson.databind.MapperFeature.DEFAULT_VIEW_INCLUSION;

/**
 * Eventz utility class
 */
class Utilz {
    private Utilz() {}

    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
        .configure(DEFAULT_VIEW_INCLUSION, false)
        .configure(FAIL_ON_UNKNOWN_PROPERTIES, false);

    /**
     * Get metadata for EventStore event
     * @param event RecordedEvent event
     * @return Tzmetadata metadata
     */
    static Tzmetadata getEventMetadata(RecordedEvent event) {
        try {
            return OBJECT_MAPPER.readValue(event.metadata, Tzmetadata.class);
        } catch (IOException e) {
            throw new RuntimeException("Cannot read event metadata", e);
        }
    }

    /**
     * Invoke method on an instance by method name
     * @param object Instance object
     * @param methodName Method name
     * @param arg Method argument
     * @return Result of the invocation
     */
    static Object invokeMethod(Object object, String methodName, Object arg) {
        Method method;
        try {
            method = object.getClass().getMethod(methodName, arg.getClass());
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
        return invokeMethod(object, method, arg);
    }

    /**
     * Invoke method on an instance
     * @param object Instance object
     * @param method Method
     * @param arg Method argument
     * @return Result of the invocation
     */
    static Object invokeMethod(Object object, Method method, Object arg) {
        try {
            method.setAccessible(true);
            return method.invoke(object, arg);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e.getCause());
        }
    }

    /**
     * Format string, a shorthand for String.format()
     * @param format Format
     * @param args Arguments
     * @return Formatted string
     */
    static String f(String format, Object... args) {
        return String.format(format, args);
    }
}
