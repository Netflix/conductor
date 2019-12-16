package com.netflix.conductor.core.execution;

import com.netflix.conductor.common.metadata.tasks.Task;

public interface TaskStatusPublisher {
    void onSyncTaskScheduled(Task task);
}
