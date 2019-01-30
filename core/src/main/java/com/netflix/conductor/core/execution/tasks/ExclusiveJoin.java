package com.netflix.conductor.core.execution.tasks;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ExclusiveJoin extends WorkflowSystemTask {
    private static Logger logger = LoggerFactory.getLogger(ExclusiveJoin.class);

    public ExclusiveJoin() {
        super("EXCLUSIVE_JOIN");
    }


    @Override
    @SuppressWarnings("unchecked")
    public boolean execute(Workflow workflow, Task task, WorkflowExecutor provider) {

        boolean allDone = false;
        boolean hasFailures = false;
        boolean useWorkflowInput = false;
        StringBuilder failureReason = new StringBuilder();
        List<String> joinOn = (List<String>) task.getInputData().get("joinOn");
        Task exclusiveTask = null ;
        for(String joinOnRef : joinOn){
            logger.debug(" Exclusive Join On Task {} ", joinOnRef);
             exclusiveTask = workflow.getTaskByRefName(joinOnRef);
            if(exclusiveTask == null){
                allDone = false;
                    continue;
            }
            Task.Status taskStatus = exclusiveTask.getStatus();
            allDone=true;
            hasFailures = !taskStatus.isSuccessful();
            if(hasFailures){
                failureReason.append(exclusiveTask.getReasonForIncompletion()).append(" ");
            }
            break;
        }

        if(!allDone)
        {
           List<String> defaultExclusiveJoinTasks =(List<String>) task.getInputData().get("defaultExclusiveJoinTask");
            if(defaultExclusiveJoinTasks!=null && !defaultExclusiveJoinTasks.isEmpty())
            {
            	for(String defaultExclusiveJoinTask : defaultExclusiveJoinTasks) {
            		//Pick the first task that we should join on and break.
	                exclusiveTask = workflow.getTaskByRefName(defaultExclusiveJoinTask);
	                if(exclusiveTask != null) {
	                    allDone = true;
	                    break;
	                }
	                else {
	                    logger.debug(" defaultExclusiveJoinTask {} is not found/executed ", defaultExclusiveJoinTask);
	                }
            	}
            }
            else
            {
                useWorkflowInput = true;
            }
        }

        if(allDone || hasFailures  || useWorkflowInput){
            if(hasFailures){
                task.setReasonForIncompletion(failureReason.toString());
                task.setStatus(Task.Status.FAILED);
            }else{
                task.setOutputData((useWorkflowInput) ? workflow.getInput() : exclusiveTask.getOutputData() );
                task.setStatus(Task.Status.COMPLETED);
            }
            return true;
        }
        return false;
    }
}