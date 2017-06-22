package com.netflix.conductor.contribs.http;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

/**
 * Created by pavanj on 6/22/17.
 */
public class HttpWaitTask extends HttpTask {

    private static final Logger logger = LoggerFactory.getLogger(HttpTask.class);
    private String requestParameter;

    @Inject
    public HttpWaitTask(RestClientManager rcm, Configuration config) {
        super(NAME, rcm, config);
    }

    public HttpWaitTask(String name, RestClientManager rcm, Configuration config) {
        super(name, rcm, config);
        logger.info("HttpWaitTask initialized...");
    }

    @Override
    public void start(Workflow workflow, Task task, WorkflowExecutor executor) throws Exception {
        Object request = task.getInputData().get(requestParameter);
        task.setWorkerId(config.getServerId());
        if(request == null) {
            String reason = MISSING_REQUEST;
            task.setReasonForIncompletion(reason);
            task.setStatus(Task.Status.FAILED);
            return;
        }

        Input input = om.convertValue(request, Input.class);
        if(input.getUri() == null) {
            String reason = "Missing HTTP URI.  See documentation for HttpTask for required input parameters";
            task.setReasonForIncompletion(reason);
            task.setStatus(Task.Status.FAILED);
            return;
        }

        if(input.getMethod() == null) {
            String reason = "No HTTP method specified";
            task.setReasonForIncompletion(reason);
            task.setStatus(Task.Status.FAILED);
            return;
        }

        try {

            HttpResponse response = httpCall(input);
            if(response.statusCode > 199 && response.statusCode < 300) {
                //task.setStatus(Task.Status.COMPLETED);
            } else {
                task.setReasonForIncompletion(response.body.toString());
                task.setStatus(Task.Status.FAILED);
            }
            if(response != null) {
                task.getOutputData().put("response", response.asMap());
            }

        }catch(Exception e) {

            logger.error(e.getMessage(), e);
            task.setStatus(Task.Status.FAILED);
            task.setReasonForIncompletion(e.getMessage());
            task.getOutputData().put("response", e.getMessage());
        }
    }
}
