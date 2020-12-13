package com.netflix.conductor.core.eventbus;

import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.EventBus;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class DecideAsyncEventBus {

    private static ExecutorService executorService =
            new ThreadPoolExecutor(5, 10, 60L, TimeUnit.SECONDS,
                    new LinkedBlockingQueue<>(1000));

    public static class EventBusHolder {

        private static EventBus BUS = new AsyncEventBus(executorService);
    }

    public static void postEvent(Object event) {
        EventBusHolder.BUS.post(event);
    }

    public static void register(Object listener) {
        EventBusHolder.BUS.register(listener);
    }

    public static void unregister(Object listener) {
        EventBusHolder.BUS.unregister(listener);
    }
}
