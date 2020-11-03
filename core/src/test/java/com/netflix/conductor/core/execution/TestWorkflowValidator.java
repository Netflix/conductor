/*
 * Copyright 2020 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.core.execution;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.common.utils.JsonMapperProvider;
import com.netflix.conductor.service.WorkflowValidator;
import com.netflix.conductor.service.WorkflowValidatorImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Viren
 */
public class TestWorkflowValidator {
	private WorkflowValidator workflowValidator;

	@Before
	public void init() {
		workflowValidator = new WorkflowValidatorImpl();
	}

	@Test
	public void testEmptyInput() throws JsonProcessingException {
		final WorkflowDef def = new WorkflowDef();
		def.setInputDefinition(generateInputDefinition());
		final Map<String, Object> inputs = new HashMap<>();
		final Workflow workflow = new Workflow();
		workflow.setInput(inputs);
		workflowValidator.validate(def, workflow);
	}

	@Test
	public void testSingleInvalidInput() throws JsonProcessingException {
		final WorkflowDef def = new WorkflowDef();
		def.setInputDefinition(generateInputDefinition());
		final Map<String, Object> inputs = new HashMap<>();
		inputs.put("person", Collections.emptyMap());
		final Workflow workflow = new Workflow();
		workflow.setInput(inputs);

		try {
			workflowValidator.validate(def, workflow);
		} catch (final ValidationException e) {
			Assert.assertEquals(1, e.getViolationCount());
			Assert.assertEquals(1, e.getErrors().size());
			Assert.assertEquals("#/person: required key [name] not found", e.getErrors().get(0));
		}
	}

	@Test
	public void testMultipleInvalidInputs() throws JsonProcessingException {
		final WorkflowDef def = new WorkflowDef();
		def.setInputDefinition(generateInputDefinition());
		final Map<String, Object> inputs = new HashMap<>();
		inputs.put("person", Collections.singletonMap("age", 11));
		final Workflow workflow = new Workflow();
		workflow.setInput(inputs);

		try {
			workflowValidator.validate(def, workflow);
		} catch (final ValidationException e) {
			Assert.assertEquals(2, e.getViolationCount());
			Assert.assertEquals(2, e.getErrors().size());
			Assert.assertEquals("#/person: required key [name] not found", e.getErrors().get(0));
			Assert.assertEquals("#/person/age: 11 is not greater or equal to 18", e.getErrors().get(1));
		}
	}

	@Test
	public void testValidInput() throws JsonProcessingException {
		final WorkflowDef def = new WorkflowDef();
		def.setInputDefinition(generateInputDefinition());
		final Map<String, Object> person = new HashMap<>();
		person.put("name", "John");
		person.put("age", 24);
		final Map<String, Object> inputs = Collections.singletonMap("person", person);
		final Workflow workflow = new Workflow();
		workflow.setInput(inputs);
		workflowValidator.validate(def, workflow);

	}

	private Map<String, Object> generateInputDefinition() throws JsonProcessingException {
		final String inputDefinition = "{"
			+ "\"type\": \"object\","
		    + "\"properties\": {"
				+ "\"person\": {"
					+ "\"type\": \"object\","
					+ "\"description\": \"Testing nested properties\","
					+ "\"required\": [\"name\"],"
					+ "\"properties\": {"
						+ "\"name\": {"
							+ "\"type\": \"string\""
						+ "},"
						+ "\"age\": {"
							+ "\"type\": \"integer\","
							+ "\"minimum\": 18,"
							+ "\"maximum\": 99"
						+ "}"
					+ "}"
				+ "}"
			+ "}"
		+ "}";
		final ObjectMapper objectMapper = new JsonMapperProvider().get();
		return objectMapper.readValue(inputDefinition, Map.class);
	}
}
