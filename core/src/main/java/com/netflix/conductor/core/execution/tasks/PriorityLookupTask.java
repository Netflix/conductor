package com.netflix.conductor.core.execution.tasks;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.core.utils.PriorityLookup;
import com.netflix.conductor.dao.PriorityLookupDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author Pradeep Palat
 */

public class PriorityLookupTask extends WorkflowSystemTask {
    private static final Logger logger = LoggerFactory.getLogger(GetConfig.class);
    private static final int DEFAULT_PRIORITY = 5;


    PriorityLookupDAO priorityLookupDAO;

    @Inject
    public PriorityLookupTask(PriorityLookupDAO priorityLookupDAO) {
        super("PRIORITY_LOOKUP");
        this.priorityLookupDAO = priorityLookupDAO;
    }

    @Override
    public void start(Workflow workflow, Task task, WorkflowExecutor executor) throws Exception {
        String priorityStr = String.valueOf(task.getInputData().getOrDefault("jobPriority", DEFAULT_PRIORITY));
        Map<String, String> priorityConfigurations = new HashMap<>();
        try{
            int priority = Integer.parseInt(priorityStr);
            List<PriorityLookup> priorityLookups = priorityLookupDAO.getPriority(priority);
            if ( priorityLookups != null){
                priorityConfigurations = priorityLookups.stream().collect(Collectors.toMap(PriorityLookup::getName, PriorityLookup::getValue));
            }

        }catch(Exception ex){
            task.setStatus(Task.Status.FAILED);
            task.setReasonForIncompletion(ex.getMessage());
            logger.debug(ex.getMessage(), ex);
        }
        task.getOutputData().put("output", priorityConfigurations);
        task.setStatus(Task.Status.COMPLETED);

    }

}
