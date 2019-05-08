/**
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * 
 */
package com.netflix.conductor.common.metadata.events;

import com.netflix.conductor.common.metadata.events.EventHandler.Action;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author Viren
 *
 */
public class EventExecution {

	public enum Status {
		IN_PROGRESS, COMPLETED, FAILED, SKIPPED
	}
	
	private String id;
	
	private String messageId;
	
	private String name;
	
	private String event;
	
	private long created;
	
	private Status status;
	
	private Action.Type action;
	
	private Map<String, Object> output = new HashMap<>();

	private String subject;

	private long received;

	private long processed;

	private long started;

	private long accepted;

	private Set<String> tags = new HashSet<>();

	public EventExecution() {
		
	}
	
	public EventExecution(String id, String messageId) {
		this.id = id;
		this.messageId = messageId;
	}
	
	/**
	 * @return the id
	 */
	public String getId() {
		return id;
	}

	/**
	 * @param id the id to set
	 * 
	 */
	public void setId(String id) {
		this.id = id;
	}

	
	/**
	 * @return the messageId
	 */
	public String getMessageId() {
		return messageId;
	}

	/**
	 * @param messageId the messageId to set
	 * 
	 */
	public void setMessageId(String messageId) {
		this.messageId = messageId;
	}

	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @param name the name to set
	 * 
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * @return the event
	 */
	public String getEvent() {
		return event;
	}

	/**
	 * @param event the event to set
	 * 
	 */
	public void setEvent(String event) {
		this.event = event;
	}

	/**
	 * @return the created
	 */
	public long getCreated() {
		return created;
	}

	/**
	 * @param created the created to set
	 * 
	 */
	public void setCreated(long created) {
		this.created = created;
	}

	/**
	 * @return the status
	 */
	public Status getStatus() {
		return status;
	}

	/**
	 * @param status the status to set
	 * 
	 */
	public void setStatus(Status status) {
		this.status = status;
	}

	/**
	 * @return the action
	 */
	public Action.Type getAction() {
		return action;
	}

	/**
	 * @param action the action to set
	 * 
	 */
	public void setAction(Action.Type action) {
		this.action = action;
	}

	/**
	 * @return the output
	 */
	public Map<String, Object> getOutput() {
		return output;
	}

	/**
	 * @param output the output to set
	 * 
	 */
	public void setOutput(Map<String, Object> output) {
		this.output = output;
	}

	/**
	 *
	 * @return Subject name
	 */
	public String getSubject() {
		return subject;
	}

	/**
	 *
	 * @param subject Subject name
	 */
	public void setSubject(String subject) {
		this.subject = subject;
	}

	/**
	 *
	 * @return Time when message received by provider
	 */
	public long getReceived() {
		return received;
	}

	/**
	 *
	 * @param received Time when message received by provider
	 */
	public void setReceived(long received) {
		this.received = received;
	}

	/**
	 *
	 * @return Time when message processed by action
	 */
	public long getProcessed() {
		return processed;
	}

	/**
	 *
	 * @param processed Time when message processed by action
	 */
	public void setProcessed(long processed) {
		this.processed = processed;
	}

	/**
	 *
	 * @return Time when action execution has been started
	 */
	public long getStarted() {
		return started;
	}

	/**
	 *
	 * @param started Time when action execution has been started
	 */
	public void setStarted(long started) {
		this.started = started;
	}

	/**
	 *
	 * @return Time when message accepted at event handler for processing
	 */
	public long getAccepted() {
		return accepted;
	}

	/**
	 *
	 * @param accepted Time when message accepted at event handler for processing
	 */
	public void setAccepted(long accepted) {
		this.accepted = accepted;
	}

	/**
	 *
	 * @return Associated tags
	 */
	public Set<String> getTags() {
		return tags;
	}

	/**
	 *
	 * @param tags Associated tags
	 */
	public void setTags(Set<String> tags) {
		this.tags = tags;
	}
}
