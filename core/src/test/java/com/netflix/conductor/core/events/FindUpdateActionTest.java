package com.netflix.conductor.core.events;

import com.netflix.conductor.core.execution.WorkflowExecutor;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class FindUpdateActionTest {

    @Test
    public void simple() throws Exception {
        FindUpdateAction findUpdateAction = new FindUpdateAction(mock(WorkflowExecutor.class));

        Map<String, Object> task = new HashMap<>();
        task.put("p", "1");

        Map<String, String> event = new HashMap<>();
        event.put("p", "1");

        boolean matches = findUpdateAction.matches(task, event, null);
        assertTrue(matches);

        matches = findUpdateAction.matches(task, event, ".task.p == .event.p");
        assertTrue(matches);
    }

    @Test
    public void compare_objects() throws Exception {
        FindUpdateAction findUpdateAction = new FindUpdateAction(mock(WorkflowExecutor.class));

        Map<String, Object> task = new HashMap<>();
        task.put("p", 1);

        Map<String, String> event = new HashMap<>();
        event.put("p", "1");

        boolean matches = findUpdateAction.matches(task, event, null);
        assertFalse(matches);

        matches = findUpdateAction.matches(task, event, ".task.p == .event.p");
        assertFalse(matches);
    }

    @Test
    public void compare_nulls() throws Exception {
        FindUpdateAction findUpdateAction = new FindUpdateAction(mock(WorkflowExecutor.class));

        Map<String, Object> task = new HashMap<>();
        Map<String, String> event = new HashMap<>();

        boolean matches = findUpdateAction.matches(task, event, null);
        assertFalse(matches);

        matches = findUpdateAction.matches(task, event, ".task == .event");
        assertTrue(matches);
    }

    @Test
    public void all_false() throws Exception {
        FindUpdateAction findUpdateAction = new FindUpdateAction(mock(WorkflowExecutor.class));

        Map<String, Object> task = new HashMap<>();

        Map<String, String> event = new HashMap<>();
        event.put("p", "1");

        boolean matches = findUpdateAction.matches(task, event, null);
        assertFalse(matches);

        matches = findUpdateAction.matches(task, event, ".task.p == .event.p");
        assertFalse(matches);
    }

    @Test
    public void ignore_extra_in_task() throws Exception {
        FindUpdateAction findUpdateAction = new FindUpdateAction(mock(WorkflowExecutor.class));

        Map<String, Object> task = new HashMap<>();
        task.put("p", "1");
        task.put("v", "1");

        Map<String, String> event = new HashMap<>();
        event.put("p", "1");

        boolean matches = findUpdateAction.matches(task, event, null);
        assertTrue(matches);

        matches = findUpdateAction.matches(task, event, ".task.p == .event.p");
        assertTrue(matches);
    }

}