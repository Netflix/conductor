/*
 * Copyright 2022 Netflix, Inc.
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
package com.netflix.conductor.core.execution.tasks;

import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

import org.apache.commons.lang3.time.DateUtils;
import org.junit.Test;

import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;

import static org.junit.Assert.*;

public class TestWait {

    private final Wait wait = new Wait();

    @Test
    public void testWaitForever() {

        TaskModel task = new TaskModel();
        task.setStatus(TaskModel.Status.SCHEDULED);
        WorkflowModel model = new WorkflowModel();

        wait.start(model, task, null);
        assertEquals(TaskModel.Status.IN_PROGRESS, task.getStatus());
        assertTrue(task.getOutputData().isEmpty());
    }

    @Test
    public void testWaitUntil() throws ParseException {
        String dateFormat = "yyyy-MM-dd HH:mm";

        WorkflowModel model = new WorkflowModel();

        TaskModel task = new TaskModel();
        task.setStatus(TaskModel.Status.SCHEDULED);

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(dateFormat);
        LocalDateTime now = LocalDateTime.now();
        String formatted = formatter.format(now);
        task.getInputData().put(Wait.UNTIL_INPUT, formatted);
        Date parsed = DateUtils.parseDate(formatted, dateFormat);

        wait.start(model, task, null);
        assertEquals(TaskModel.Status.IN_PROGRESS, task.getStatus());
        assertFalse(task.getOutputData().isEmpty());
        assertTrue(task.getOutputData().containsKey(Wait.TIMEOUT));
        assertEquals(parsed.getTime(), task.getOutputData().get(Wait.TIMEOUT));

        // Execute runs when checking if the task has completed
        boolean updated = wait.execute(model, task, null);
        assertTrue(updated);
        assertEquals(TaskModel.Status.COMPLETED, task.getStatus());
    }

    @Test
    public void testWaitDuration() throws ParseException {
        WorkflowModel model = new WorkflowModel();

        TaskModel task = new TaskModel();
        task.setStatus(TaskModel.Status.SCHEDULED);

        task.getInputData().put(Wait.DURATION_INPUT, "1s");
        wait.start(model, task, null);
        long now = System.currentTimeMillis();

        assertEquals(TaskModel.Status.IN_PROGRESS, task.getStatus());
        assertFalse(task.getOutputData().isEmpty());
        assertTrue(task.getOutputData().containsKey(Wait.TIMEOUT));
        assertEquals(now + 1000, task.getOutputData().get(Wait.TIMEOUT));

        try {
            Thread.sleep(2_000);
        } catch (InterruptedException e) {
        }

        // Execute runs when checking if the task has completed
        boolean updated = wait.execute(model, task, null);
        assertTrue(updated);
        assertEquals(TaskModel.Status.COMPLETED, task.getStatus());
    }

    @Test
    public void testInvalidWaitConfig() throws ParseException {
        WorkflowModel model = new WorkflowModel();

        TaskModel task = new TaskModel();
        task.setStatus(TaskModel.Status.SCHEDULED);

        task.getInputData().put(Wait.DURATION_INPUT, "1s");
        task.getInputData().put(Wait.UNTIL_INPUT, "2022-12-12");
        wait.start(model, task, null);
        assertEquals(TaskModel.Status.FAILED, task.getStatus());
        assertTrue(!task.getReasonForIncompletion().isEmpty());
    }
}
