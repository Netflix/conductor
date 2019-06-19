package com.netflix.conductor.server.resources;

import com.netflix.conductor.common.metadata.workflow.StartWorkflowRequest;
import com.netflix.conductor.service.WorkflowService;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

public class FloResourceTest {

	@Mock
	private WorkflowService mockWorkflowService;

	@Before
	public void before() {
		this.mockWorkflowService = Mockito.mock(WorkflowService.class);
	}

	@Test
	public void testStartWorkflow() {
		FloResource floResource = new FloResource(null,mockWorkflowService);
		StartWorkflowRequest startWorkflowRequest = new StartWorkflowRequest();
		startWorkflowRequest.setName("w123");
		Map<String, Object> input = new HashMap<>();
		input.put("1", "abc");

		String workflowID = "w112";

		ArgumentCaptor<Map> captor = ArgumentCaptor.forClass(Map.class);
		when(mockWorkflowService.startWorkflow(Mockito.anyString(),Mockito.anyInt(),Mockito.anyString(),captor.capture())).thenReturn(workflowID);
		assertEquals("w112", floResource.startWorkflow("test",1,"","testTraceId",input));
		assertEquals(captor.getValue().get("tracingId"),"testTraceId");
	}

	@Test
	public void testStartWorkflowWithoutTracingIdInHeader() {
		FloResource floResource = new FloResource(null,mockWorkflowService);
		StartWorkflowRequest startWorkflowRequest = new StartWorkflowRequest();
		startWorkflowRequest.setName("w123");
		Map<String, Object> input = new HashMap<>();
		input.put("1", "abc");

		String workflowID = "w112";

		ArgumentCaptor<Map> captor = ArgumentCaptor.forClass(Map.class);
		when(mockWorkflowService.startWorkflow(Mockito.anyString(),Mockito.anyInt(),Mockito.anyString(),captor.capture())).thenReturn(workflowID);
		assertEquals("w112", floResource.startWorkflow("test",1,"",null,input));
		assertNotNull(captor.getValue().get("tracingId"));
	}

}
