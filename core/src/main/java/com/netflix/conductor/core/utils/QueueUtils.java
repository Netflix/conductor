/**
 * Copyright 2016 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.conductor.core.utils;

import com.netflix.conductor.common.metadata.tasks.Task;
import org.apache.commons.lang3.StringUtils;

/**
 *
 * @author visingh
 *
 */
public class QueueUtils {

    public static final String DOMAIN_SEPARATOR = ":";
    public static final String ISOLATION_SEPARATOR = "-";

    public static String getQueueName(Task task) {
        return getQueueName(task.getTaskType(), task.getDomain(),task.getIsolationGroupId());
    }

    public static String getQueueName(
      String taskType, String domain, String isolationGroup) {

        String queueName = null;
        if (domain == null) {
            queueName = taskType;
        } else {
            queueName = domain + DOMAIN_SEPARATOR + taskType;
        }
        if(isolationGroup != null) {
            queueName = queueName + ISOLATION_SEPARATOR + isolationGroup;
        }
        return queueName;
    }

    public static String getQueueNameWithoutDomain(String queueName) {
        return queueName.substring(queueName.indexOf(DOMAIN_SEPARATOR) + 1);
    }

    public static String getQueueDomain(String queueName) {
        if(StringUtils.contains(queueName,DOMAIN_SEPARATOR)) {
            return StringUtils.substringBefore(queueName,DOMAIN_SEPARATOR);
        }
        return StringUtils.EMPTY;
    }


    public static boolean isIsolatedQueue(String queue) {
        return StringUtils.isNotBlank(getIsolationGroup(queue));
    }

    private static String getIsolationGroup(String queue) {
        return StringUtils.substringAfter(queue, QueueUtils.ISOLATION_SEPARATOR);
    }

    public static String getTaskType(String queue) {
        String queueWithoutIsolationGroup = StringUtils.substringBeforeLast(queue, ISOLATION_SEPARATOR);
        if(StringUtils.contains(queueWithoutIsolationGroup,DOMAIN_SEPARATOR)) {
            return StringUtils.substringAfterLast(queueWithoutIsolationGroup, DOMAIN_SEPARATOR);
        }
        return queueWithoutIsolationGroup;
    }
}
