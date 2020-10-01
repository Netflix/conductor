/*
 * Copyright 2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.dao.dynomite;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.Task.Status;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.utils.JsonMapperProvider;
import com.netflix.conductor.config.TestConfiguration;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.dao.ExecutionDAOTest;
import com.netflix.conductor.dao.SemaphoreDAO;
import com.netflix.conductor.dao.redis.JedisMock;
import com.netflix.conductor.dyno.DynoProxy;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import redis.clients.jedis.commands.JedisCommands;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Viren
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class RedisExecutionDAOTest extends ExecutionDAOTest {

    private RedisExecutionDAO executionDAO;
	private static ObjectMapper objectMapper = new JsonMapperProvider().get();

    @Before
    public void init() {
        Configuration config = new TestConfiguration();
        JedisCommands jedisMock = new JedisMock();
        DynoProxy dynoClient = new DynoProxy(jedisMock);
        SemaphoreDAO sem = new RedisSemaphoreDAO(dynoClient, objectMapper, config);

        executionDAO = new RedisExecutionDAO(dynoClient, objectMapper, config, sem);
    }

    @Test
    public void testCorrelateTaskToWorkflowInDS() {
        String workflowId = "workflowId";
        String taskId = "taskId1";
        String taskDefName = "task1";

        TaskDef def = new TaskDef();
        def.setName("task1");
        def.setConcurrentExecLimit(1);

        Task task = new Task();
        task.setTaskId(taskId);
        task.setWorkflowInstanceId(workflowId);
        task.setReferenceTaskName("ref_name");
        task.setTaskDefName(taskDefName);
        task.setTaskType(taskDefName);
        task.setStatus(Status.IN_PROGRESS);
        List<Task> tasks = executionDAO.createTasks(Collections.singletonList(task));
        assertNotNull(tasks);
        assertEquals(1, tasks.size());

        executionDAO.correlateTaskToWorkflowInDS(taskId, workflowId);
        tasks = executionDAO.getTasksForWorkflow(workflowId);
        assertNotNull(tasks);
        assertEquals(workflowId, tasks.get(0).getWorkflowInstanceId());
        assertEquals(taskId, tasks.get(0).getTaskId());
    }

    @Test
    public void ExecLimitTest() {
        // setup
        TaskDef def = new TaskDef();
        def.setTimeoutSeconds(3600);
        WorkflowTask wfTask = new WorkflowTask();
        wfTask.setTaskDefinition(def);
        Task task = new Task();
        task.setReferenceTaskName("referenceTaskName");
        task.setWorkflowTask(wfTask);
        
        // global
        task.setWorkflowType("gWorkflowType");
        wfTask.setGlobalConcurrentExecutionLimit(3);
        for (int i = 1; i < 4; i++) {
            task.setTaskId(String.valueOf(i));
            assertFalse(
				String.format("Initial try acquire %d/3", i),
				executionDAO.exceedsInProgressLimit(task)
			);
        }
        task.setTaskId("4");
        assertTrue(
            "Don't allow more global concurrent executions than specified",
            executionDAO.exceedsInProgressLimit(task)
        );
        task.setWorkflowType("gWorkflowType2");
        assertTrue(
            "Global setting actually works across different workflows",
            executionDAO.exceedsInProgressLimit(task)
        );
        task.setWorkflowType("gWorkflowType");
        task.setTaskId("2");
        task.setStatus(Status.COMPLETED);
        executionDAO.updateTask(task);
        task.setTaskId("4");
        task.setStatus(Status.IN_PROGRESS);
        assertFalse(
            "Completing task No1 lets task No4 run",
            executionDAO.exceedsInProgressLimit(task)
        );
        wfTask.setGlobalConcurrentExecutionLimit(0);

        task.setWorkflowType("workflowType");
        // local
        wfTask.setLocalConcurrentExecutionLimit(3);
        for (int i = 1; i < 4; i++) {
            task.setTaskId(String.valueOf(i));
            assertFalse(
				String.format("Initial try acquire %d/3", i),
				executionDAO.exceedsInProgressLimit(task)
			);
        }
        task.setTaskId("4");
        assertTrue(
            "Don't allow more local concurrent executions than specified",
            executionDAO.exceedsInProgressLimit(task)
        );
        task.setWorkflowType("workflowType2");
        assertFalse(
            "Local setting is not global",
            executionDAO.exceedsInProgressLimit(task)
        );
        task.setWorkflowType("workflowType");
        task.setTaskId("2");
        task.setStatus(Status.COMPLETED);
        executionDAO.updateTask(task);
        task.setTaskId("4");
        task.setStatus(Status.IN_PROGRESS);
        assertFalse(
            "Completing task No1 lets task No4 run",
            executionDAO.exceedsInProgressLimit(task)
        );
        wfTask.setLocalConcurrentExecutionLimit(0);
    }

    @Override
    protected ExecutionDAO getExecutionDAO() {
        return executionDAO;
    }
}
