package lt.tazkazz.eventz;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Eventz utility class
 */
class Utilz {
    private Utilz() {}

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
