package com.netflix.conductor.core.events;

import com.netflix.conductor.core.execution.WorkflowExecutor;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class FindUpdateActionTest {
    private FindUpdateAction findUpdateAction = new FindUpdateAction(mock(WorkflowExecutor.class));

    @Test
    public void simple() throws Exception {
        Map<String, Object> task = new HashMap<>();
        task.put("p", "1");

        Map<String, Object> event = new HashMap<>();
        event.put("p", "1");

        boolean matches = findUpdateAction.matches(task, event, null);
        assertTrue(matches);

        matches = findUpdateAction.matches(task, event, ".task.p == .event.p");
        assertTrue(matches);
    }

    @Test
    public void compare_objects() throws Exception {
        Map<String, Object> task = new HashMap<>();
        task.put("p", Collections.singletonMap("foo", "bar"));

        Map<String, Object> event = new HashMap<>();
        event.put("p", Collections.singletonMap("foo", "bar"));

        boolean matches = findUpdateAction.matches(task, event, null);
        assertTrue(matches);

        matches = findUpdateAction.matches(task, event, ".task.p == .event.p");
        assertTrue(matches);

        event.put("p", Collections.singletonMap("foo", "bar2"));
        matches = findUpdateAction.matches(task, event, ".task.p == .event.p");
        assertFalse(matches);
    }

    @Test
    public void compare_nulls() throws Exception {
        Map<String, Object> task = new HashMap<>();
        Map<String, Object> event = new HashMap<>();

        boolean matches = findUpdateAction.matches(task, event, null);
        assertFalse(matches);

        matches = findUpdateAction.matches(task, event, ".task == .event");
        assertTrue(matches);
    }

    @Test
    public void all_false() throws Exception {
        Map<String, Object> task = new HashMap<>();

        Map<String, Object> event = new HashMap<>();
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

        Map<String, Object> event = new HashMap<>();
        event.put("p", "1");

        boolean matches = findUpdateAction.matches(task, event, null);
        assertTrue(matches);

        matches = findUpdateAction.matches(task, event, ".task.p == .event.p");
        assertTrue(matches);
    }

    @Test
    public void match_integer_params_true() throws Exception {
        FindUpdateAction findUpdateAction = new FindUpdateAction(mock(WorkflowExecutor.class));

        Map<String, Object> task = new HashMap<>();
        task.put("p", 1);

        Map<String, Object> event = new HashMap<>();
        event.put("p", 1);

        boolean matches = findUpdateAction.matches(task, event, null);
        assertTrue(matches);
    }

    @Test
    public void match_integer_params_false() throws Exception {
        FindUpdateAction findUpdateAction = new FindUpdateAction(mock(WorkflowExecutor.class));

        Map<String, Object> task = new HashMap<>();
        task.put("p", 1);

        Map<String, Object> event = new HashMap<>();
        event.put("p", 2);

        boolean matches = findUpdateAction.matches(task, event, null);
        assertFalse(matches);
    }

    @Test
    public void match_integer_params_with_string_true() throws Exception {
        FindUpdateAction findUpdateAction = new FindUpdateAction(mock(WorkflowExecutor.class));

        Map<String, Object> task = new HashMap<>();
        task.put("p", 1);

        Map<String, Object> event = new HashMap<>();
        event.put("p", "1");

        boolean matches = findUpdateAction.matches(task, event, null);
        assertTrue(matches);
    }

    @Test
    public void match_integer_params_with_string_false() throws Exception {
        FindUpdateAction findUpdateAction = new FindUpdateAction(mock(WorkflowExecutor.class));

        Map<String, Object> task = new HashMap<>();
        task.put("p", 1);

        Map<String, Object> event = new HashMap<>();
        event.put("p", "2");

        boolean matches = findUpdateAction.matches(task, event, null);
        assertFalse(matches);
    }
}