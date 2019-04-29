/**
 * Copyright 2016 Netflix, Inc.
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
package com.netflix.conductor.common.metadata.tasks;

import com.github.vmg.protogen.annotations.ProtoEnum;
import com.github.vmg.protogen.annotations.ProtoField;
import com.github.vmg.protogen.annotations.ProtoMessage;
import com.netflix.conductor.common.constraints.TaskTimeoutConstraint;
import com.netflix.conductor.common.metadata.Auditable;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author Viren
 * Defines a workflow task definition 
 */
@ProtoMessage
@TaskTimeoutConstraint
@Valid
public class TaskDef extends Auditable {
	@ProtoEnum
	public static enum TimeoutPolicy {RETRY, TIME_OUT_WF, ALERT_ONLY}

	@ProtoEnum
	public static enum RetryLogic {FIXED, EXPONENTIAL_BACKOFF}

	private static final int ONE_HOUR = 60 * 60;

	/**
	 * Unique name identifying the task.  The name is unique across
	 */
	@NotEmpty(message = "TaskDef name cannot be null or empty")
	@ProtoField(id = 1)
	private String name;

	@ProtoField(id = 2)
	private String description;

	@ProtoField(id = 3)
	@Min(value = 0, message = "TaskDef retryCount: {value} must be >= 0")
	private int retryCount = 3; // Default

	@ProtoField(id = 4)
	@NotNull
	private long timeoutSeconds;

	@ProtoField(id = 5)
	private List<String> inputKeys = new ArrayList<String>();

	@ProtoField(id = 6)
	private List<String> outputKeys = new ArrayList<String>();

	@ProtoField(id = 7)
	private TimeoutPolicy timeoutPolicy = TimeoutPolicy.TIME_OUT_WF;

	@ProtoField(id = 8)
	private RetryLogic retryLogic = RetryLogic.FIXED;

	@ProtoField(id = 9)
	private int retryDelaySeconds = 60;

	@ProtoField(id = 10)
	@Min(value = 1, message = "TaskDef responseTimeoutSeconds: ${validatedValue} should be minimum {value} second")
	private long responseTimeoutSeconds = ONE_HOUR;

	@ProtoField(id = 11)
	private Integer concurrentExecLimit;

	@ProtoField(id = 12)
	private Map<String, Object> inputTemplate = new HashMap<>();

	// This field is deprecated, do not use id 13.
//	@ProtoField(id = 13)
//	private Integer rateLimitPerSecond;

	@ProtoField(id = 14)
	private Integer rateLimitPerFrequency;

	@ProtoField(id = 15)
	private Integer rateLimitFrequencyInSeconds;

	public TaskDef() {
	}

	public TaskDef(String name) {
		this.name = name;
	}

	public TaskDef(String name, String description) {
		this.name = name;
		this.description = description;
	}

	public TaskDef(String name, String description, int retryCount, int timeout) {
		this.name = name;
		this.description = description;
		this.retryCount = retryCount;
		this.timeoutSeconds = timeout;
	}

	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @param name the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * @return the description
	 */
	public String getDescription() {
		return description;
	}

	/**
	 * @param description the description to set
	 */
	public void setDescription(String description) {
		this.description = description;
	}

	/**
	 * @return the retryCount
	 */
	public int getRetryCount() {
		return retryCount;
	}

	/**
	 * @param retryCount the retryCount to set
	 */
	public void setRetryCount(int retryCount) {
		this.retryCount = retryCount;
	}

	/**
	 * @return the timeoutSeconds
	 */
	public long getTimeoutSeconds() {
		return timeoutSeconds;
	}

	/**
	 * @param timeoutSeconds the timeoutSeconds to set
	 */
	public void setTimeoutSeconds(long timeoutSeconds) {
		this.timeoutSeconds = timeoutSeconds;
	}

	/**
	 *
	 * @return Returns the input keys
	 */
	public List<String> getInputKeys() {
		return inputKeys;
	}

	/**
	 * @param inputKeys Set of keys that the task accepts in the input map
	 */
	public void setInputKeys(List<String> inputKeys) {
		this.inputKeys = inputKeys;
	}

	/**
	 * @return Returns the output keys for the task when executed
	 */
	public List<String> getOutputKeys() {
		return outputKeys;
	}

	/**
	 * @param outputKeys Sets the output keys
	 */
	public void setOutputKeys(List<String> outputKeys) {
		this.outputKeys = outputKeys;
	}


	/**
	 * @return the timeoutPolicy
	 */
	public TimeoutPolicy getTimeoutPolicy() {
		return timeoutPolicy;
	}

	/**
	 * @param timeoutPolicy the timeoutPolicy to set
	 */
	public void setTimeoutPolicy(TimeoutPolicy timeoutPolicy) {
		this.timeoutPolicy = timeoutPolicy;
	}

	/**
	 * @return the retryLogic
	 */
	public RetryLogic getRetryLogic() {
		return retryLogic;
	}

	/**
	 * @param retryLogic the retryLogic to set
	 */
	public void setRetryLogic(RetryLogic retryLogic) {
		this.retryLogic = retryLogic;
	}

	/**
	 * @return the retryDelaySeconds
	 */
	public int getRetryDelaySeconds() {
		return retryDelaySeconds;
	}

	/**
	 *
	 * @return the timeout for task to send response.  After this timeout, the task will be re-queued
	 */
	public long getResponseTimeoutSeconds() {
		return responseTimeoutSeconds;
	}

	/**
	 *
	 * @param responseTimeoutSeconds - timeout for task to send response.  After this timeout, the task will be re-queued
	 */
	public void setResponseTimeoutSeconds(long responseTimeoutSeconds) {
		this.responseTimeoutSeconds = responseTimeoutSeconds;
	}

	/**
	 * @param retryDelaySeconds the retryDelaySeconds to set
	 */
	public void setRetryDelaySeconds(int retryDelaySeconds) {
		this.retryDelaySeconds = retryDelaySeconds;
	}

	/**
	 * @return the inputTemplate
	 */
	public Map<String, Object> getInputTemplate() {
		return inputTemplate;
	}


	/**
	 *
	 * @return rateLimitPerFrequency The max number of tasks that will be allowed to be executed per rateLimitFrequencyInSeconds.
	 */
	public Integer getRateLimitPerFrequency() {
		return rateLimitPerFrequency == null ? 0 : rateLimitPerFrequency;
	}

	/**
	 *
	 * @param rateLimitPerFrequency The max number of tasks that will be allowed to be executed per rateLimitFrequencyInSeconds.
	 *                                 Setting the value to 0 removes the rate limit
	 */
	public void setRateLimitPerFrequency(Integer rateLimitPerFrequency) {
		this.rateLimitPerFrequency = rateLimitPerFrequency;
	}

	/**
	 * @return rateLimitFrequencyInSeconds: The time bucket that is used to rate limit tasks based on {@link #getRateLimitPerFrequency()}
	 * If null or not set, then defaults to 1 second
	 */
	public Integer getRateLimitFrequencyInSeconds() {
		return rateLimitFrequencyInSeconds == null ? 1 : rateLimitFrequencyInSeconds;
	}

	/**
	 *
	 * @param rateLimitFrequencyInSeconds: The time window/bucket for which the rate limit needs to be applied. This will only have affect if {@link #getRateLimitPerFrequency()} is greater than zero
	 */
	public void setRateLimitFrequencyInSeconds(Integer rateLimitFrequencyInSeconds) {
		this.rateLimitFrequencyInSeconds = rateLimitFrequencyInSeconds;
	}

	/**
	 *
	 * @param concurrentExecLimit Limit of number of concurrent task that can be  IN_PROGRESS at a given time.  Seting the value to 0 removes the limit.
	 */
	public void setConcurrentExecLimit(Integer concurrentExecLimit) {
		this.concurrentExecLimit = concurrentExecLimit;
	}

	/**
	 *
	 * @return Limit of number of concurrent task that can be  IN_PROGRESS at a given time
	 */
	public Integer getConcurrentExecLimit() {
		return concurrentExecLimit;
	}
	/**
	 *
	 * @return concurrency limit
	 */
	public int concurrencyLimit() {
		return concurrentExecLimit == null ? 0 : concurrentExecLimit.intValue();
	}

	/**
	 * @param inputTemplate the inputTemplate to set
	 *
	 */
	public void setInputTemplate(Map<String, Object> inputTemplate) {
		this.inputTemplate = inputTemplate;
	}

	@Override
	public String toString(){
		return name;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		TaskDef taskDef = (TaskDef) o;
		return getRetryCount() == taskDef.getRetryCount() &&
				getTimeoutSeconds() == taskDef.getTimeoutSeconds() &&
				getRetryDelaySeconds() == taskDef.getRetryDelaySeconds() &&
				getResponseTimeoutSeconds() == taskDef.getResponseTimeoutSeconds() &&
				Objects.equals(getName(), taskDef.getName()) &&
				Objects.equals(getDescription(), taskDef.getDescription()) &&
				Objects.equals(getInputKeys(), taskDef.getInputKeys()) &&
				Objects.equals(getOutputKeys(), taskDef.getOutputKeys()) &&
				getTimeoutPolicy() == taskDef.getTimeoutPolicy() &&
				getRetryLogic() == taskDef.getRetryLogic() &&
				Objects.equals(getConcurrentExecLimit(), taskDef.getConcurrentExecLimit()) &&
				Objects.equals(getRateLimitPerFrequency(), taskDef.getRateLimitPerFrequency()) &&
				Objects.equals(getInputTemplate(), taskDef.getInputTemplate()) &&
				Objects.equals(getIsolationGroupId(), taskDef.getIsolationGroupId());
	}

	@Override
	public int hashCode() {

		return Objects.hash(getName(), getDescription(), getRetryCount(), getTimeoutSeconds(), getInputKeys(),
				getOutputKeys(), getTimeoutPolicy(), getRetryLogic(), getRetryDelaySeconds(),
				getResponseTimeoutSeconds(), getConcurrentExecLimit(), getRateLimitPerFrequency(), getInputTemplate(), getIsolationGroupId());
	}

	@ProtoField(id = 99)
	private String isolationGroupId;

	public String getIsolationGroupId() {
		return isolationGroupId;
	}

	public void setIsolationGroupId(String isolationGroupId) {
		this.isolationGroupId = isolationGroupId;
	}
}
