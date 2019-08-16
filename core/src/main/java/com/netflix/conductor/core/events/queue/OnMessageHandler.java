package com.netflix.conductor.core.events.queue;

/**
 * @author Oleksiy Lysak
 */
@FunctionalInterface
public interface OnMessageHandler {
	void apply(ObservableQueue queue, Message msg);
}
