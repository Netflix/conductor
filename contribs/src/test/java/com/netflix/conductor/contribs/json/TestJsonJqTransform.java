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
package com.netflix.conductor.contribs.json;

import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
/**
 * @author Oleksiy Lysak
 *
 */
public class TestJsonJqTransform {
	private JsonJqTransform jqTransform = new JsonJqTransform();
	private WorkflowExecutor executorMock = mock(WorkflowExecutor.class);
	private Workflow workflowMock = mock(Workflow.class);

	@Test
	public void missingExpression() throws Exception {
		Task task = new Task();

		jqTransform.start(workflowMock, task, executorMock);
		assertEquals(Task.Status.FAILED, task.getStatus());
		assertEquals("Missing 'queryExpression' in input parameters", task.getReasonForIncompletion());
	}

	@Test
	public void runtimeException() throws Exception {
		Task task = new Task();
		task.getInputData().put("queryExpression", "WRONG");

		jqTransform.start(workflowMock, task, executorMock);
		assertEquals(Task.Status.FAILED, task.getStatus());
		assertEquals("Function WRONG/0 does not exist", task.getReasonForIncompletion());

		assertNotNull(task.getOutputData().get("error"));
		assertEquals(1, task.getOutputData().size());
		assertEquals("Function WRONG/0 does not exist", task.getOutputData().get("error").toString());
	}

	@Test
	public void bothNullNode() throws Exception {
		Task task = new Task();
		task.getInputData().put("queryExpression", ".foo");

		jqTransform.start(workflowMock, task, executorMock);
		assertEquals(Task.Status.COMPLETED, task.getStatus());
		assertEquals(2, task.getOutputData().size());

		Object resultObj = task.getOutputData().get("result");
		assertTrue(resultObj instanceof NullNode);

		Object resultListObj= task.getOutputData().get("resultList");
		assertTrue(resultListObj instanceof List);
		List resultList = ((List)resultListObj);

		assertEquals(1, resultList.size());
		assertTrue(resultList.get(0) instanceof NullNode);
	}

	@Test
	public void bothValue() throws Exception {
		Task task = new Task();
		task.getInputData().put("foo", "bar");
		task.getInputData().put("queryExpression", ".foo");

		jqTransform.start(workflowMock, task, executorMock);
		assertEquals(Task.Status.COMPLETED, task.getStatus());
		assertEquals(2, task.getOutputData().size());

		Object resultObj = task.getOutputData().get("result");
		assertTrue(resultObj instanceof TextNode);
		assertEquals("bar", ((TextNode)resultObj).asText());

		Object resultListObj= task.getOutputData().get("resultList");
		assertTrue(resultListObj instanceof List);
		List resultList = ((List)resultListObj);

		assertEquals(1, resultList.size());
		assertTrue(resultList.get(0) instanceof TextNode);
		assertEquals("bar", ((TextNode)resultList.get(0)).asText());
	}

	@Test
	public void suppressSingle() throws Exception {
		Task task = new Task();
		task.getInputData().put("foo", 1);
		task.getInputData().put("suppressSingle", "true");
		task.getInputData().put("queryExpression", ".foo");

		jqTransform.start(workflowMock, task, executorMock);
		assertEquals(Task.Status.COMPLETED, task.getStatus());
		assertEquals(1, task.getOutputData().size());

		assertNull(task.getOutputData().get("result"));

		Object resultListObj= task.getOutputData().get("resultList");
		assertTrue(resultListObj instanceof List);
		List resultList = ((List)resultListObj);

		assertEquals(1, resultList.size());
		assertTrue(resultList.get(0) instanceof IntNode);
		assertEquals(1, ((IntNode)resultList.get(0)).intValue());
	}

	@Test
	public void suppressList() throws Exception {
		Task task = new Task();
		task.getInputData().put("foo", 1);
		task.getInputData().put("suppressList", "true");
		task.getInputData().put("queryExpression", ".foo");

		jqTransform.start(workflowMock, task, executorMock);
		assertEquals(Task.Status.COMPLETED, task.getStatus());
		assertEquals(1, task.getOutputData().size());

		assertNull(task.getOutputData().get("resultList"));

		Object resultObj = task.getOutputData().get("result");
		assertTrue(resultObj instanceof IntNode);
		assertEquals(1, ((IntNode)resultObj).intValue());
	}
}