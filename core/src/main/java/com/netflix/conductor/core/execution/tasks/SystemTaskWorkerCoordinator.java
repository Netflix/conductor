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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.service.MetricService;
import org.apache.commons.collections.CollectionUtils;
import org.apache.log4j.NDC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.*;

/**
 * @author Viren
 *
 */
@Singleton
public class SystemTaskWorkerCoordinator {

	private static Logger logger = LoggerFactory.getLogger(SystemTaskWorkerCoordinator.class);
	
	private QueueDAO taskQueues;
	
	private WorkflowExecutor executor;
	
	private ExecutorService es;
	
	private int workerQueueSize;
	
	private int pollCount;

	private int pollTimeout;

	private long pollFrequency;

	private LinkedBlockingQueue<Runnable> workerQueue;
	
	private int unackTimeout;
	
	private Configuration config;
	
	private static BlockingQueue<WorkflowSystemTask> queue = new LinkedBlockingQueue<>();
	
	private static Set<WorkflowSystemTask> listeningTasks = new HashSet<>();
	
	private static final String className = SystemTaskWorkerCoordinator.class.getName();

	private String workerId;
		
	@Inject
	public SystemTaskWorkerCoordinator(QueueDAO taskQueues, WorkflowExecutor executor, Configuration config) {
		this.taskQueues = taskQueues;
		this.executor = executor;
		this.config = config;
		this.unackTimeout = config.getIntProperty("workflow.system.task.worker.callback.seconds", 30);
		int threadCount = config.getIntProperty("workflow.system.task.worker.thread.count", 5);
		this.pollCount = config.getIntProperty("workflow.system.task.worker.poll.count", 5);
		this.pollTimeout = config.getIntProperty("workflow.system.task.worker.poll.timeout", 500);
		this.pollFrequency = config.getIntProperty("workflow.system.task.worker.poll.frequency", 500);
		this.workerQueueSize = config.getIntProperty("workflow.system.task.worker.queue.size", 100);
		this.workerQueue = new LinkedBlockingQueue<Runnable>(workerQueueSize);
		if(threadCount > 0) {
			ThreadFactory tf = new ThreadFactoryBuilder().setNameFormat("system-task-worker-%d").build();
			this.es = new ThreadPoolExecutor(threadCount, threadCount,
	                0L, TimeUnit.MILLISECONDS,
	                workerQueue,
	                tf);

			new Thread(()->listen()).start();
			logger.debug("System Task Worker Initialized with {} threads and a callback time of {} second and queue size {} with pollCount {}", threadCount, unackTimeout, workerQueueSize, pollCount);
		} else {
			logger.warn("System Task Worker DISABLED");
		}
		try {
			this.workerId = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			this.workerId = "unknown";
		}
	}

	static synchronized void add(WorkflowSystemTask systemTask) {
		logger.debug("Adding system task {}", systemTask.getName());
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
		Executors.newScheduledThreadPool(1).scheduleWithFixedDelay(()->pollAndExecute(systemTask), 1000, pollFrequency, TimeUnit.MILLISECONDS);
		logger.debug("Started listening {}", systemTask.getName());
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
			String lockQueue = name.toLowerCase() + ".lock";
			List<String> polled = taskQueues.pop(name, pollCount, pollTimeout);
			if (CollectionUtils.isNotEmpty(polled)) {
				MetricService.getInstance().taskPoll(name, workerId, polled.size());
			}
			logger.debug("Polling for {}, got {}", name, polled.size());
			for(String task : polled) {
				try {
					es.submit(()-> {
						NDC.push("system-"+ UUID.randomUUID().toString());

						// This prevents another containers executing the same action
						// true means this session added the record to lock queue and can start the task
						long expireTime = systemTask.getRetryTimeInSecond() * 2L; // 2 times longer than task retry time
						boolean locked = taskQueues.pushIfNotExists(lockQueue, task, expireTime);
						if (!locked) {
							logger.warn("Cannot lock the task " + task);
							MetricService.getInstance().taskLockFailed(name);
							return;
						}

						try {
							executor.executeSystemTask(systemTask, task, unackTimeout);
						} finally {
							NDC.remove();
							taskQueues.remove(lockQueue, task);
						}
					});
				}catch(RejectedExecutionException ree) {
					logger.warn("Queue full for workers {}, taskId {}", workerQueue.size(), task);
					MetricService.getInstance().systemWorkersQueueFull(name);
				}
			}
			
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}
	
}	
