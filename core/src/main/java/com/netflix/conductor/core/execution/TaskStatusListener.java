package com.netflix.conductor.core.execution;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.netflix.conductor.common.metadata.tasks.Task;

public interface TaskStatusListener {
    void onTaskScheduled(Task task);
}
