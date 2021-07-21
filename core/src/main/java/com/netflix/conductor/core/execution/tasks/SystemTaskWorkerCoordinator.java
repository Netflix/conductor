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
import java.util.*;
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

	private final Map<String, ScheduledExecutorService> taskPools = new HashMap<>();

	private String workerId;

	private boolean useTaskLock;
		
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
		this.useTaskLock = Boolean.parseBoolean(config.getProperty("workflow.system.task.use.lock", "true"));
	}

	static synchronized void add(WorkflowSystemTask systemTask) {
		logger.debug("Adding system task {}", systemTask.getName());
		queue.add(systemTask);
	}

	public void shutdown() {
		for (Map.Entry<String, ScheduledExecutorService> pool : taskPools.entrySet()) {
			try {
				logger.info("Closing task pool " + pool.getKey());
				pool.getValue().shutdown();
				pool.getValue().awaitTermination(5, TimeUnit.SECONDS);
			} catch (Exception e) {
				logger.debug("Closing task pool " + pool.getKey() + " failed " + e.getMessage(), e);
			}
		}
		try {
			if (es != null) {
				logger.info("Closing executor pool");
				es.shutdown();
				es.awaitTermination(5, TimeUnit.SECONDS);
			}
		} catch (Exception e) {
			logger.debug("Closing executor pool failed " + e.getMessage(), e);
		}
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
		ScheduledExecutorService taskPool = Executors.newScheduledThreadPool(1);
		taskPool.scheduleWithFixedDelay(()->pollAndExecute(systemTask), 1000, pollFrequency, TimeUnit.MILLISECONDS);
		logger.debug("Started listening {}", systemTask.getName());
		taskPools.put(systemTask.getName(), taskPool);
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

			// Returns the number of additional elements that this queue can ideally
			// (in the absence of memory or resource constraints) accept without blocking.
			// This is always equal to the initial capacity of this queue less the current size of this queue.
			int remainingCapacity = workerQueue.remainingCapacity();

			// Grab either remaining worker's queue capacity or the poll count per configuration
			// In the high load, it basically picks only what can process
			int effectivePollCount = Math.min(remainingCapacity, pollCount);

			String name = systemTask.getName();
			String lockQueue = name.toLowerCase() + ".lock";
			List<String> polled = taskQueues.pop(name, effectivePollCount, pollTimeout);
			if (CollectionUtils.isNotEmpty(polled)) {
				MetricService.getInstance().taskPoll(name, workerId, polled.size());
			}
			logger.trace("Polling for {}, got {}", name, polled.size());
			for(String task : polled) {
				try {
					es.submit(()-> {
						NDC.push("system-"+ UUID.randomUUID().toString());

						// This workaround was applied some time ago as somehow task id picked up multiple times at close same time
						// Adding this option to manage it per environment as eventually we need to find root cause of the issue
						if (useTaskLock) {
							long expireTime = systemTask.getRetryTimeInSecond() * 2L; // 2 times longer than task retry time
							boolean locked = taskQueues.pushIfNotExists(lockQueue, task, expireTime);
							// This prevents another containers executing the same action
							// true means this session added the record to lock queue and can start the task
							if (!locked) {
								logger.warn("Cannot lock the task " + task);
								MetricService.getInstance().taskLockFailed(name);
								return;
							}
						}

						try {
							executor.executeSystemTask(systemTask, task, unackTimeout);
						} finally {
							NDC.remove();
							taskQueues.remove(lockQueue, task);
						}
					});
				}catch(RejectedExecutionException ree) {
					taskQueues.unpop(name, task); //Unpop it back so other cluster instance might pick it up
					logger.warn("Queue full for workers {}, taskId {}", workerQueue.size(), task);
					MetricService.getInstance().systemWorkersQueueFull(name);
				}
			}
			
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}
	
}	
