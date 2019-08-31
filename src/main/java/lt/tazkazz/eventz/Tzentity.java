package lt.tazkazz.eventz;

import java.util.List;

import static lt.tazkazz.eventz.Utilz.invokeMethod;

/**
 * Tzentity entity abstract implementation
 * @param <T> Tzentity entity
 * @param <C> Tzcommand base entity command
 */
public abstract class Tzentity<T extends Tzentity, C extends Tzcommand> {
    /**
     * Apply event on the entity
     * @param event Tzevent event
     */
    protected final void applyEvent(Tzevent event) {
        invokeMethod(this, "apply", event);
    }

    /**
     * Process command on the entity
     * @param command Tzcommand command
     * @return Tzevent events
     */
    protected final List<Tzevent> processCommand(C command) {
        return (List<Tzevent>) invokeMethod(this, "process", command);
    }
}
