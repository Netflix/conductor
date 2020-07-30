package com.netflix.conductor.core.events.queue;

import com.netflix.conductor.common.metadata.events.EventExecution;

/**
 * Holds the information necessary to see context of an event failure while processing an event message
 * @author rickfish
 *
 */
public class MessageEventFailure {
	private Message message;
	private EventExecution eventExecution;
	public MessageEventFailure(Message message, EventExecution eventExecution) {
		this.message = message;
		this.eventExecution = eventExecution;
	}
	public Message getMessage() {
		return message;
	}
	public void setMessage(Message message) {
		this.message = message;
	}
	public EventExecution getEventExecution() {
		return eventExecution;
	}
	public void setEventExecution(EventExecution eventExecution) {
		this.eventExecution = eventExecution;
	}
}
