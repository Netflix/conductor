/**
 * 
 */
package com.netflix.conductor.core.execution.tasks;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.Task.Status;
import com.netflix.conductor.common.metadata.tasks.TaskResult;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.metrics.Monitors;
import com.netflix.conductor.service.ExecutionService;

/**
 * @author Viren
 *
 */
@Singleton
public class AsyncTaskWorkerCoordinator {

	private static Logger logger = LoggerFactory.getLogger(AsyncTaskWorkerCoordinator.class);
	
	private ExecutionService executionService;
	
	private WorkflowExecutor executor;
	
	private ExecutorService es;
	
	private long unackTimeout;
	
	private String workerId;
	
	private static BlockingQueue<WorkflowSystemTask> queue = new LinkedBlockingQueue<>();
	
	private static Set<WorkflowSystemTask> listeningTasks = new HashSet<>();
	
	private static final String className = AsyncTaskWorkerCoordinator.class.getName();
		
	@Inject
	public AsyncTaskWorkerCoordinator(ExecutionService executionService, WorkflowExecutor executor, Configuration config) {
		this.executionService = executionService;
		this.executor = executor;
		this.workerId = config.getServerId();
		this.unackTimeout = config.getIntProperty("workflow.async.task.worker.callback.seconds", 30);
		int threadCount = config.getIntProperty("workflow.async.task.worker.thread.count", 1);		
		if(threadCount > 0) {
			this.es = Executors.newFixedThreadPool(threadCount, new ThreadFactoryBuilder().setNameFormat("async-worker-%d").build());
			new Thread(()->listen()).start();
			logger.info("Async Task Worker Initialized with {} threads and a callback time of {} second", threadCount, unackTimeout);
		} else {
			logger.info("Async Task Worker DISABLED");
		}
	}

	static synchronized void add(WorkflowSystemTask systemTask) {
		queue.add(systemTask);
	}
	
	private void listen() {
		try {
			for(;;) {
				WorkflowSystemTask st = queue.poll(10, TimeUnit.SECONDS);
				if(st != null && st.isAsync() && !listeningTasks.contains(st)) {
					listen(st);
					listeningTasks.add(st);
				}
			}
		}catch(InterruptedException ie) {
			logger.warn(ie.getMessage(), ie);
		}
	}
	
	private void listen(WorkflowSystemTask systemTask) {
		Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(()->pollAndExecute(systemTask), 500, 500, TimeUnit.MILLISECONDS);
		logger.info("Started listening {}", systemTask.getName());
	}

	private void pollAndExecute(WorkflowSystemTask systemTask) {
		try {
			String name = systemTask.getName();
			List<Task> polled = executionService.poll(name, workerId, 1, 500);
			polled.forEach(task -> es.submit(()->execute(systemTask, task)));
		} catch (Exception e) {
			Monitors.error(className, "pollAndExecute");
			logger.error(e.getMessage(), e);
		}
	}

	private void execute(WorkflowSystemTask systemTask, Task task) {
		logger.info("Executing {}/{}-{}", task.getTaskType(), task.getTaskId(), task.getStatus());
		try {
			
			String workflowId = task.getWorkflowInstanceId();
			Workflow workflow = executionService.getExecutionStatus(workflowId, true);
			if(task.getPollCount() < 2) {
				systemTask.start(workflow, task, executor);
			} else {
				systemTask.execute(workflow, task, executor);
			}
			
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			task.setStatus(Status.FAILED);
			task.setReasonForIncompletion(e.getMessage());
		}
		task.setWorkerId(workerId);
		if(!task.getStatus().isTerminal()) {
			task.setCallbackAfterSeconds(unackTimeout);
		}

		try {
			executor.updateTask(new TaskResult(task));
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
		logger.info("Done, Executing {}/{}-{} op={}", task.getTaskType(), task.getTaskId(), task.getStatus(), task.getOutputData().toString());
	}

	
	
}	
