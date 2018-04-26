package com.netflix.conductor.core.utils;

import com.netflix.conductor.common.metadata.tasks.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskUtils {
	private static Logger logger = LoggerFactory.getLogger(TaskUtils.class);

	public static Task.Status getTaskStatus(String status) {
		try {
			return Task.Status.valueOf(status.toUpperCase());
		} catch (Exception ex) {
			logger.error("getTaskStatus: failed with " + ex.getMessage() + " for " + status);
		}
		return null;
	}
}
