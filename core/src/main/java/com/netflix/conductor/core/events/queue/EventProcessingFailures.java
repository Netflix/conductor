package com.netflix.conductor.core.events.queue;

import java.util.ArrayList;
import java.util.List;

import com.netflix.conductor.common.metadata.events.EventExecution;
/**
 * Contains two lists that are populated when processing events: transientFailures and failures. Failures are normal 
 * failures that occur because of a problem that will probably not be fixed ever on a retry. transientFailures might go
 * away if the same event is tried again.
 * 
 * @author rickfish
 *
 */
public class EventProcessingFailures {
	private List<EventExecution> transientFailures = new ArrayList<EventExecution>();
	private List<EventExecution> failures = new ArrayList<EventExecution>();
	public boolean isEmpty() {
		return this.transientFailures.isEmpty() && this.failures.isEmpty();
	}
	public List<EventExecution> getTransientFailures() {
		return transientFailures;
	}
	public void setTransientFailures(List<EventExecution> transientFailures) {
		this.transientFailures = transientFailures;
	}
	public List<EventExecution> getFailures() {
		return failures;
	}
	public void setFailures(List<EventExecution> failures) {
		this.failures = failures;
	}
}
