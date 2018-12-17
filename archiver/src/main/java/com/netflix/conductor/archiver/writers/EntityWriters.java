package com.netflix.conductor.archiver.writers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class EntityWriters {
    private Map<String, FileWrapper> wrappers = new HashMap<>();

    private final static String RUNTIME = "RUNTIME";
    private final static String TASK_LOGS = "TASK_LOGS";
    private final static String EXECUTIONS = "EXECUTIONS";
    private final static String WORKFLOWS = "WORKFLOWS";

    public EntityWriters(String parent, boolean startFrom) throws IOException {
        wrappers.put(RUNTIME, new FileWrapper(parent + "/runtime.json", false, startFrom));

        wrappers.put(TASK_LOGS, new FileWrapper(parent + "/task_logs.json", false, startFrom));

        wrappers.put(WORKFLOWS, new FileWrapper(parent + "/workflows.json", true, startFrom));

        wrappers.put(EXECUTIONS, new FileWrapper(parent + "/executions.json", false, startFrom));
    }

    public void RUNTIME(String hit) throws IOException {
        wrappers.get(RUNTIME).write(hit);
    }

    public void TASK_LOGS(String hit) throws IOException {
        wrappers.get(TASK_LOGS).write(hit);
    }

    public void WORKFLOWS(String hit) throws IOException {
        wrappers.get(WORKFLOWS).write(hit);
    }

    public void EXECUTIONS(String hit) throws IOException {
        wrappers.get(EXECUTIONS).write(hit);
    }

    public void close() {
        wrappers.values().forEach(FileWrapper::close);
    }

    public Map<String, FileWrapper> wrappers() {
        return wrappers;
    }
}
