/**
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.metrics.Monitors;
import com.netflix.conductor.service.ExecutionService;

/**
 * @author Viren
 *
 */
@Singleton
public class SystemTaskWorkerCoordinator {

	private static Logger logger = LoggerFactory.getLogger(SystemTaskWorkerCoordinator.class);
	
	private ExecutionService executionService;
	
	private WorkflowExecutor executor;
	
	private ExecutorService es;
	
	private int workerQueueSize;
	
	private LinkedBlockingQueue<Runnable> workerQueue;
	
	private int unackTimeout;
	
	private String workerId;
	
	private Configuration config;
	
	private static BlockingQueue<WorkflowSystemTask> queue = new LinkedBlockingQueue<>();
	
	private static Set<WorkflowSystemTask> listeningTasks = new HashSet<>();
	
	private static final String className = SystemTaskWorkerCoordinator.class.getName();
		
	@Inject
	public SystemTaskWorkerCoordinator(ExecutionService executionService, WorkflowExecutor executor, Configuration config) {
		this.executionService = executionService;
		this.executor = executor;
		this.config = config;
		this.workerId = config.getServerId();
		this.unackTimeout = config.getIntProperty("workflow.system.task.worker.callback.seconds", 30);
		int threadCount = config.getIntProperty("workflow.system.task.worker.thread.count", 5);
		int workerQueueSize = config.getIntProperty("workflow.system.task.worker.queue.size", 100);
		this.workerQueue = new LinkedBlockingQueue<Runnable>(workerQueueSize);
		if(threadCount > 0) {
			ThreadFactory tf = new ThreadFactoryBuilder().setNameFormat("system-task-worker-%d").build();
			this.es = new ThreadPoolExecutor(threadCount, threadCount,
	                0L, TimeUnit.MILLISECONDS,
	                workerQueue,
	                tf);

			new Thread(()->listen()).start();
			logger.info("System Task Worker Initialized with {} threads and a callback time of {} second", threadCount, unackTimeout);
		} else {
			logger.info("System Task Worker DISABLED");
		}
	}

	static synchronized void add(WorkflowSystemTask systemTask) {
		logger.info("Adding system task {}", systemTask.getName());
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
		Executors.newScheduledThreadPool(1).scheduleWithFixedDelay(()->pollAndExecute(systemTask), 1000, 500, TimeUnit.MILLISECONDS);
		logger.info("Started listening {}", systemTask.getName());
	}

	private void pollAndExecute(WorkflowSystemTask systemTask) {
		try {
			
			if(config.disableAsyncWorkers()) {
				logger.warn("System Task Worker is DISABLED.  Not polling.");
				return;
			}
			
			if(workerQueue.size() >= workerQueueSize) {
				logger.warn("All workers are busy, not polling.  queue size {}, max {}", workerQueue.size(), workerQueueSize);
				return;
			}
			
			String name = systemTask.getName();
			List<Task> polled = executionService.justPoll(name, 10, 1000);
			logger.debug("Polling for {}, got {}", name, polled.size());
			for(Task task : polled) {
				try {
					es.submit(()->executor.executeSystemTask(systemTask, task, workerId, unackTimeout));
				}catch(RejectedExecutionException ree) {
					logger.warn("Queue full for workers {}", workerQueue.size());
				}
			}
			
		} catch (Exception e) {
			Monitors.error(className, "pollAndExecute");
			logger.error(e.getMessage(), e);
		}
	}
	
}	
