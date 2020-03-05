/*
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
package com.netflix.conductor.core.execution;

import com.netflix.conductor.core.config.Configuration;

import java.util.Map;

/**
 * @author Viren
 *
 */
public interface TestConfiguration extends Configuration {

	@Override
	default int getSweepFrequency() {
		return 1;
	}

	@Override
	default boolean disableSweep() {
		return false;
	}

	@Override
	default boolean disableAsyncWorkers() {
		return false;
	}

	@Override
	default boolean isEventMessageIndexingEnabled() {
		return true;
	}

	@Override
	default String getServerId() {
		return "server_id";
	}

	@Override
	default String getEnvironment() {
		return "test";
	}

	@Override
	default String getStack() {
		return "junit";
	}

	@Override
	default String getAppId() {
		return "workflow";
	}

	@Override
	default boolean enableAsyncIndexing() {
		return true;
	}

	@Override
	default String getProperty(String string, String def) {
		return "dummy";
	}

    @Override
    default boolean getBooleanProperty(String name, boolean defaultValue) {
        return false;
    }

    @Override
	default String getAvailabilityZone() {
		return "us-east-1a";
	}

	@Override
	default int getIntProperty(String string, int def) {
		return 100;
	}

	@Override
	default String getRegion() {
		return "us-east-1";
	}

	@Override
	default Long getWorkflowInputPayloadSizeThresholdKB() {
		return 10L;
	}

	@Override
	default Long getMaxWorkflowInputPayloadSizeThresholdKB() {
		return 10240L;
	}

	@Override
	default Long getWorkflowOutputPayloadSizeThresholdKB() {
		return 10L;
	}

	@Override
	default Long getMaxWorkflowOutputPayloadSizeThresholdKB() {
		return 10240L;
	}

	@Override
	default Long getTaskInputPayloadSizeThresholdKB() {
		return 10L;
	}

	@Override
	default Long getMaxTaskInputPayloadSizeThresholdKB() {
		return 10240L;
	}

	@Override
	default Long getTaskOutputPayloadSizeThresholdKB() {
		return 10L;
	}

	@Override
	default Long getMaxTaskOutputPayloadSizeThresholdKB() {
		return 10240L;
	}

	@Override
	default Map<String, Object> getAll() {
		return null;
	}

	@Override
	default long getLongProperty(String name, long defaultValue) {
		return 1000000L;
	}
}
