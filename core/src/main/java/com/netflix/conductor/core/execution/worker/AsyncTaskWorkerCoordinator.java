/**
 * 
 */
package com.netflix.conductor.core.execution.worker;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.Task.Status;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import com.netflix.conductor.core.events.queue.dyno.DynoEventQueueProvider;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.core.execution.tasks.WorkflowSystemTask;
import com.netflix.conductor.dao.ExecutionDAO;

/**
 * @author Viren
 *
 */
@Singleton
public class AsyncTaskWorkerCoordinator {

	private static Logger logger = LoggerFactory.getLogger(AsyncTaskWorkerCoordinator.class);
	
	private DynoEventQueueProvider queueProvider;
	
	private ExecutionDAO edao;
	
	private WorkflowExecutor executor;
	
	private ExecutorService es;
	
	private long unackTimeout = 30_1000;	//30 second!
	
	@Inject
	public AsyncTaskWorkerCoordinator(DynoEventQueueProvider queueProvider, ExecutionDAO edao, WorkflowExecutor executor, Configuration config) {
		this.queueProvider = queueProvider;
		this.edao = edao;
		this.executor = executor;
		
		//TODO: Find a better of doing this!
		Executors.newSingleThreadScheduledExecutor().schedule(() -> {
			
			List<WorkflowSystemTask> asyncTasks = WorkflowSystemTask.all().stream().filter(stt -> stt.isAsync()).collect(Collectors.toList());
			int threadCount = config.getIntProperty("workflow.async.task.worker.thread.count", asyncTasks.size());
			if(threadCount > 0) {
				this.es = Executors.newFixedThreadPool(threadCount);
				asyncTasks.forEach(task -> listen(task));
				logger.info("Async Task Worker Initialized with {} threads", threadCount);
			} else {
				logger.info("Async Task Worker DISABLED");
			}
			
		}, 1000, TimeUnit.MINUTES);
		
	}

	private void listen(WorkflowSystemTask systemTask) {
		String name = systemTask.getName();
		ObservableQueue queue = queueProvider.getQueue(name);
		queue.observe().subscribe((Message msg) -> handle(systemTask, msg, queue));		
	}

	private void handle(WorkflowSystemTask systemTask, Message msg, ObservableQueue queue) {
		es.submit(()->_handle(systemTask, msg, queue));
	}
	
	private void _handle(WorkflowSystemTask systemTask, Message msg, ObservableQueue queue) {
		
		String taskId = msg.getId();
		Task task = edao.getTask(taskId);
		String workflowId = task.getWorkflowInstanceId();
		Workflow workflow = edao.getWorkflow(workflowId, true);
		try {
			switch(task.getStatus()) {
				case SCHEDULED:
					systemTask.start(workflow, task, executor);
					break;
				case IN_PROGRESS:
					systemTask.execute(workflow, task, executor);
					break;
				default:
					//do nothing					
			}
			
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			task.setStatus(Status.FAILED);
			task.setReasonForIncompletion(e.getMessage());
		}
		edao.updateTask(task);
		
		if(task.getStatus().isTerminal()) {
			queue.ack(Arrays.asList(msg));	
		} else {
			queue.setUnackTimeout(msg, unackTimeout);
		}
		
		try {
			executor.decide(workflowId);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}
	
}	
