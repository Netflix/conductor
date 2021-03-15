/**
 * Copyright 2017 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 *
 */
package com.netflix.conductor.core.execution.tasks;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.dao.MetadataDAO;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Optional;

/**
 * @author Oleksiy Lysak
 */

public class GetConfig extends WorkflowSystemTask {
	private static final Logger logger = LoggerFactory.getLogger(GetConfig.class);
	private final Configuration config;
	private MetadataDAO metadataDAO;

	@Inject

	public GetConfig(MetadataDAO metadataDAO, Configuration config) {
		super("GET_CONFIG");
		this.config = config;
		this.metadataDAO = metadataDAO;
	}

	@Override
	public void start(Workflow workflow, Task task, WorkflowExecutor executor) throws Exception {
		try {
			String configName = (String) task.getInputData().get("name");
			String source = (String) task.getInputData().getOrDefault("source", "db");

			if (StringUtils.isEmpty(configName))
				throw new IllegalArgumentException("No config name provided in parameters");

			if ( "vault".equalsIgnoreCase(source)){
				task.getOutputData().put("result", config.getProperty(configName, ""));
			}else{
				Optional<Pair<String, String>> entry = metadataDAO.getConfigs().stream()
						.filter(p -> configName.equalsIgnoreCase(p.getLeft()))
						.findFirst();

				String defaultValue = (String) task.getInputData().get("defaultValue");
				if (entry.isPresent()) {
					String effectiveValue = StringUtils.defaultIfEmpty(entry.get().getRight(), defaultValue);
					task.getOutputData().put("result", effectiveValue);
				} else {
					task.getOutputData().put("result", defaultValue);
				}
			}

			task.setStatus(Task.Status.COMPLETED);
		} catch (Exception e) {
			task.setStatus(Task.Status.FAILED);
			task.setReasonForIncompletion(e.getMessage());
			logger.debug(e.getMessage(), e);
		}
	}
}