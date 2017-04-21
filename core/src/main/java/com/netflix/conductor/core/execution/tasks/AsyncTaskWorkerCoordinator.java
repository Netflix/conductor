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
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.tasks.TaskResult;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.metrics.Monitors;
import com.netflix.conductor.service.ExecutionService;
import com.netflix.conductor.service.MetadataService;

/**
 * @author Viren
 *
 */
@Singleton
public class AsyncTaskWorkerCoordinator {

	private static Logger logger = LoggerFactory.getLogger(AsyncTaskWorkerCoordinator.class);
	
	private ExecutionService executionService;
	
	private ExecutionDAO dao;
	
	private WorkflowExecutor executor;
	
	private ExecutorService es;
	
	private MetadataService ms;
	
	private long unackTimeout;
	
	private String workerId;
	
	private static BlockingQueue<WorkflowSystemTask> queue = new LinkedBlockingQueue<>();
	
	private static Set<WorkflowSystemTask> listeningTasks = new HashSet<>();
	
	private static final String className = AsyncTaskWorkerCoordinator.class.getName();
		
	@Inject
	public AsyncTaskWorkerCoordinator(ExecutionService executionService, MetadataService ms, ExecutionDAO dao, WorkflowExecutor executor, Configuration config) {
		this.executionService = executionService;
		this.ms = ms;
		this.dao = dao;
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
				WorkflowSystemTask st = queue.poll(60, TimeUnit.SECONDS);
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
			List<Task> polled = executionService.justPoll(name, 1, 500);
			polled.forEach(task -> es.submit(()->execute(systemTask, task)));
		} catch (Exception e) {
			Monitors.error(className, "pollAndExecute");
			logger.error(e.getMessage(), e);
		}
	}

	//TODO: ACK!!!
	private void execute(WorkflowSystemTask systemTask, Task task) {
		logger.info("Executing {}/{}-{}", task.getTaskType(), task.getTaskId(), task.getStatus());
		try {
			
			TaskDef taskDef = ms.getTaskDef(task.getTaskDefName());
			int limit = 0;
			if(taskDef != null) {
				limit = taskDef.getConcurrencyLimit();
			}			
			if(limit > 0 && dao.rateLimited(task, limit)) {
				logger.warn("Rate limited for {}", task.getTaskDefName());
				return;
			}
			
			String workflowId = task.getWorkflowInstanceId();
			Workflow workflow = executionService.getExecutionStatus(workflowId, true);			
			
			if (task.getStartTime() == 0) {
				task.setStartTime(System.currentTimeMillis());
				Monitors.recordQueueWaitTime(task.getTaskDefName(), task.getQueueWaitTime());
			}
			
			task.setWorkerId(workerId);
			task.setPollCount(task.getPollCount() + 1);
			dao.updateTask(task);
			
			switch (task.getStatus()) {
				case SCHEDULED:
					systemTask.start(workflow, task, executor);
					break;
				case IN_PROGRESS:
					systemTask.execute(workflow, task, executor);
					break;
				default:
					break;
			}
			
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			task.setStatus(Status.FAILED);
			task.setReasonForIncompletion(e.getMessage());
		}
		
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
