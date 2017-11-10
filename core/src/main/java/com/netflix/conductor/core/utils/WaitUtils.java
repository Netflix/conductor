package com.netflix.conductor.core.utils;

import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * @author Oleksiy Lysak
 */
public class WaitUtils {
    private static Logger logger = LoggerFactory.getLogger(WaitUtils.class);

    /**
     * The waiter until the service is up.
     *
     * @param service  The service name. e.g. 'redis', 'elasticsearch'.
     * @param attempts The number of attempts to be performed before giving up.
     * @param sleep    The number of seconds to wait until apply for the next attempt.
     * @param supplier Function should return true if service is up, false if not or throw an exception.
     */
    public static void wait(String service, int attempts, int sleep, Supplier<Boolean> supplier) {
        logger.info("Initializing {} waiter. Connection attempts {}, sleep time {} seconds", service, attempts, sleep);

        int attemptsMade = 0;
        boolean connected = false;
        do {
            attemptsMade++;
            try {
                connected = supplier.get();
                if (!connected) {
                    logger.warn("No success response from supplier. Sleep for a while. Attempts made {}", attemptsMade);
                    Uninterruptibles.sleepUninterruptibly(sleep, TimeUnit.SECONDS);
                }
            } catch (Exception ex) {
                logger.error("{} waiter failed: {}. Sleep for a while. Attempts made {}",
                        service, ex.getMessage(), attemptsMade, ex);
                Uninterruptibles.sleepUninterruptibly(sleep, TimeUnit.SECONDS);
            }
        } while (!connected && attemptsMade < attempts);

        // Print give up
        if (!connected && attemptsMade >= attempts) {
            logger.warn("No {} desired state obtained during {} attempts. Giving up ...", service, attemptsMade);
            System.exit(-1);
        }
    }
}
