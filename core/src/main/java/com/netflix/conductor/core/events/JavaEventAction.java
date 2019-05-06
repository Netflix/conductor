package com.netflix.conductor.core.events;

import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.metadata.events.EventHandler;

import java.util.Collections;
import java.util.List;

public interface JavaEventAction {
	default List<?> handle(EventHandler.Action action, Object payload, EventExecution ee) throws Exception {
		return Collections.emptyList();
	}
}
