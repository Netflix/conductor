/*
 * Copyright 2018 Netflix, Inc.
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
package com.netflix.conductor.service;

import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.ValidationException;
import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;

import java.util.List;


public class WorkflowValidatorImpl implements WorkflowValidator {
	@Override
	public void validate(WorkflowDef workflowDef, Workflow workflow) {
		if (workflowDef.getInputDefinition().isEmpty()) {
			return;
		}
		final JSONObject jsonSchema = new JSONObject(workflow.getInput());
		final JSONObject jsonSchemaDefinition = new JSONObject(workflowDef.getInputDefinition());
		final Schema schema = SchemaLoader.builder().draftV7Support()
				.schemaJson(jsonSchemaDefinition).build().load().build();
		try {
			schema.validate(jsonSchema);
		} catch (final org.everit.json.schema.ValidationException e) {
			final int violationCount = e.getViolationCount();
			final String errorMessage = e.getErrorMessage();
			final List<String> errors = e.getAllMessages();
			throw new ValidationException(errorMessage, violationCount, errors);
		}
	}
}
