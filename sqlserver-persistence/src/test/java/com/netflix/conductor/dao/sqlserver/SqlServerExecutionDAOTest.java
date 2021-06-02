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
package com.netflix.conductor.dao.sqlserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.dao.ExecutionDAOTest;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

@SuppressWarnings("Duplicates")
public class SqlServerExecutionDAOTest extends ExecutionDAOTest {

    private SqlServerDAOTestUtil testUtil;
    private SqlServerExecutionDAO executionDAO;

    @Rule
    public TestName name = new TestName();

    @Before
    public void setup() throws Exception {
        testUtil = new SqlServerDAOTestUtil(name.getMethodName());
        executionDAO = new SqlServerExecutionDAO(
                testUtil.getObjectMapper(),
                testUtil.getDataSource()
        );
        testUtil.resetAllData();
    }

    @After
    public void teardown() {
        testUtil.resetAllData();
        testUtil.getDataSource().close();
    }

    @Test
    public void testPendingByCorrelationId() {

        WorkflowDef def = new WorkflowDef();
        def.setName("pending_count_correlation_jtest");

        Workflow workflow = createTestWorkflow();
        workflow.setWorkflowDefinition(def);

        generateWorkflows(workflow, 10);

        List<Workflow> bycorrelationId = getExecutionDAO().getWorkflowsByCorrelationId("pending_count_correlation_jtest", "corr001", true);
        assertNotNull(bycorrelationId);
        assertEquals(10, bycorrelationId.size());
    }

    @Test
    @Override
    public void testTaskExceedsLimit() {
        TaskDef taskDefinition = new TaskDef();
        taskDefinition.setName("task1");
        taskDefinition.setConcurrentExecLimit(1);

        WorkflowTask workflowTask = new WorkflowTask();
        workflowTask.setName("task1");
        workflowTask.setTaskDefinition(taskDefinition);
        workflowTask.setTaskDefinition(taskDefinition);

        List<Task> tasks = new LinkedList<>();
        for (int i = 0; i < 15; i++) {
            Task task = new Task();
            task.setScheduledTime(1L);
            task.setSeq(i + 1);
            task.setTaskId(UUID.randomUUID().toString());
            task.setWorkflowInstanceId(UUID.randomUUID().toString());
            task.setReferenceTaskName("task1");
            task.setTaskDefName("task1");
            tasks.add(task);
            task.setStatus(Task.Status.SCHEDULED);
            task.setWorkflowTask(workflowTask);
        }

        getExecutionDAO().createTasks(tasks);
        assertFalse(getExecutionDAO().exceedsInProgressLimit(tasks.get(0)));
        tasks.get(0).setStatus(Task.Status.IN_PROGRESS);
        getExecutionDAO().updateTask(tasks.get(0));

        for (Task task : tasks) {
            assertTrue(getExecutionDAO().exceedsInProgressLimit(task));
        }
    }

    @Override
    public ExecutionDAO getExecutionDAO() {
        return executionDAO;
    }
}
