package com.netflix.conductor.contribs.progress;

import com.netflix.conductor.common.metadata.events.EventHandler;
import com.netflix.conductor.core.events.JavaEventAction;

import javax.inject.Singleton;
import java.util.List;

@Singleton
public class HybrikProgressHandler implements JavaEventAction {

	@Override
	public List<?> handle(EventHandler.Action action, Object payload, String event, String messageId) throws Exception {
		return null;
	}

	private static class ActionParams {

	}
}
