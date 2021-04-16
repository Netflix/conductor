package com.netflix.conductor.core.execution.tasks;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.ErrorLookup;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.dao.ErrorLookupDAO;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.text.MessageFormat;
import java.util.Optional;

/**
 * @author Pradeep Palat
 */

public class ErrorLookupTask extends WorkflowSystemTask {
    private static final Logger logger = LoggerFactory.getLogger(GetConfig.class);

    final String NO_MAPPING = "No Detailed Message for this error was found.";
    final String formattedErr = "{0}: {1}: Resolution: {2}";

    ErrorLookupDAO errorLookupDAO;

    @Inject
    public ErrorLookupTask(ErrorLookupDAO errorLookupDAO) {
        super("ERROR_LOOKUP");
        this.errorLookupDAO = errorLookupDAO;
    }

    @Override
    public void start(Workflow workflow, Task task, WorkflowExecutor executor) throws Exception {
        String lookupMessage = (String)task.getInputData().get("lookup");
        String workflow_name = (String)task.getInputData().get("workflow_name");

        if (StringUtils.isEmpty(lookupMessage)){
            throw new IllegalArgumentException("Error Message for Lookup is null.");
        }

        String error = NO_MAPPING;
        String errorCode = "COND-999";
        try{
            Optional<ErrorLookup> errorLookupOpt  = errorLookupDAO.getErrorMatching(workflow_name, lookupMessage).stream().findFirst();
            if (errorLookupOpt.isPresent()){
                ErrorLookup errorLookup = errorLookupOpt.get();
                errorCode = errorLookup.getErrorCode();
                error = getFormattedError(errorLookup);
            }
        }catch(Exception ex){
            error = "Error occured when trying to lookup the detailed message. " + ex.getMessage();
        }
        task.getOutputData().put("errorCode", errorCode);
        task.getOutputData().put("error", error);
        task.setStatus(Task.Status.COMPLETED);
    }

    private String getFormattedError( ErrorLookup errorLookup){
        return MessageFormat.format(formattedErr, errorLookup.getErrorCode(), errorLookup.getGeneralMessage(), errorLookup.getResolution());
    }
}
