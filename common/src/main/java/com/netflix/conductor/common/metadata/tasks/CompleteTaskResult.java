package com.netflix.conductor.common.metadata.tasks;

import com.github.vmg.protogen.annotations.ProtoEnum;
import com.github.vmg.protogen.annotations.ProtoField;
import com.github.vmg.protogen.annotations.ProtoMessage;

@ProtoMessage
public class CompleteTaskResult {

    @ProtoEnum
    public enum CompletionStatus {
        NO_IN_PROGRESS_WAIT_TASK_FOUND_IN_WF(-1, "No IN_PROGRESS WAIT Task found"),
        COMPLETED(0, "Task completed"),
        NO_IN_PROGRESS_WAIT_TASK_FOUND_FOR_STATE(1, "No IN_PROGRESS WAIT Task found for state");

        private int code;
        private String message;

        CompletionStatus(int code, String message) {
            this.code = code;
            this.message = message;
        }
    }

    @ProtoField(id = 1)
    private int code;

    @ProtoField(id = 2)
    private String message;

    @ProtoField(id = 3)
    private Task inProgressTask;

    public CompleteTaskResult(CompletionStatus completionStatus, Task inProgressTask) {
        this.code = completionStatus.code;
        this.message = completionStatus.message;
        this.inProgressTask = inProgressTask;
    }

    public CompleteTaskResult() {
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Task getInProgressTask() {
        return inProgressTask;
    }

    public void setInProgressTask(Task inProgressTask) {
        this.inProgressTask = inProgressTask;
    }
}
