package com.netflix.conductor.core.events;

import com.netflix.conductor.common.metadata.events.EventHandler;

public interface JavaEventAction {
	void handle(EventHandler.Action action, Object payload, String event, String messageId) throws Exception;
}
