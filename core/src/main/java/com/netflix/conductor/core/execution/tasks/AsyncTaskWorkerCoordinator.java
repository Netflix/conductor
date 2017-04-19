/**
 * 
 */
package com.netflix.conductor.core.execution.tasks;

import java.util.HashSet;
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
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import com.netflix.conductor.core.events.queue.dyno.DynoEventQueueProvider;
import com.netflix.conductor.core.execution.WorkflowExecutor;
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
	
	private long unackTimeout;
	
	private String workerId;
	
	private static BlockingQueue<WorkflowSystemTask> queue = new LinkedBlockingQueue<>();
	
	private static Set<WorkflowSystemTask> listeningTasks = new HashSet<>();
		
	@Inject
	public AsyncTaskWorkerCoordinator(DynoEventQueueProvider queueProvider, ExecutionDAO edao, WorkflowExecutor executor, Configuration config) {
		this.queueProvider = queueProvider;
		this.edao = edao;
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
		String name = systemTask.getName();
		ObservableQueue queue = queueProvider.getQueue(name);
		logger.info("Started listening {}", name);
		queue.observe().subscribe((Message msg) -> handle(systemTask, msg, queue));		
	}

	private void handle(WorkflowSystemTask systemTask, Message msg, ObservableQueue queue) {
		es.submit(()->_handle(systemTask, msg, queue));
	}
	
	private void _handle(WorkflowSystemTask systemTask, Message msg, ObservableQueue queue) {
		String taskId = msg.getId();
		Task task = edao.getTask(taskId);
		logger.info("Executing {}/{}-{}", queue.getName(), msg.getId(), task.getStatus());
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
		task.setWorkerId(workerId);
		task.setCallbackAfterSeconds(unackTimeout);

		try {
			executor.updateTask(new TaskResult(task));
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
		logger.info("Done, Executing {}/{}", queue.getName(), msg.getId());
	}

	
	
}	
