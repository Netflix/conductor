package com.netflix.conductor.core.events;

import com.netflix.conductor.common.metadata.events.EventHandler;

import java.util.Map;

public interface JavaEventAction {
	Map<String, Object> handle(EventHandler.Action action, Object payload, String event, String messageId) throws Exception;
}
