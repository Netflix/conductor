/*
 * Copyright 2022 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.core.execution.tasks;

import java.text.ParseException;
import java.time.Duration;
import java.util.Date;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.stereotype.Component;

import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;

import static com.netflix.conductor.common.metadata.tasks.TaskType.TASK_TYPE_WAIT;
import static com.netflix.conductor.model.TaskModel.Status.*;

@Component(TASK_TYPE_WAIT)
public class Wait extends WorkflowSystemTask {

    public static final String DURATION_INPUT = "duration";
    public static final String UNTIL_INPUT = "until";
    // public static final String TIMEOUT = "timeout";

    private static final String[] patterns =
            new String[] {"yyyy-MM-dd HH:mm", "yyyy-MM-dd HH:mm z", "yyyy-MM-dd"};

    public Wait() {
        super(TASK_TYPE_WAIT);
    }

    public static Duration parseDuration(String text) {
        Matcher m =
                Pattern.compile(
                                "\\s*(?:(\\d+)\\s*(?:days?|d))?"
                                        + "\\s*(?:(\\d+)\\s*(?:hours?|hrs?|h))?"
                                        + "\\s*(?:(\\d+)\\s*(?:minutes?|mins?|m))?"
                                        + "\\s*(?:(\\d+)\\s*(?:seconds?|secs?|s))?"
                                        + "\\s*",
                                Pattern.CASE_INSENSITIVE)
                        .matcher(text);
        if (!m.matches()) throw new IllegalArgumentException("Not valid duration: " + text);

        int days = (m.start(1) == -1 ? 0 : Integer.parseInt(m.group(1)));
        int hours = (m.start(2) == -1 ? 0 : Integer.parseInt(m.group(2)));
        int mins = (m.start(3) == -1 ? 0 : Integer.parseInt(m.group(3)));
        int secs = (m.start(4) == -1 ? 0 : Integer.parseInt(m.group(4)));
        return Duration.ofSeconds((days * 86400) + (hours * 60L + mins) * 60L + secs);
    }

    public static Date parseDate(String date) throws ParseException {
        return DateUtils.parseDate(date, patterns);
    }

    @Override
    public void start(WorkflowModel workflow, TaskModel task, WorkflowExecutor workflowExecutor) {

        String duration =
                Optional.ofNullable(task.getInputData().get(DURATION_INPUT)).orElse("").toString();
        String until =
                Optional.ofNullable(task.getInputData().get(UNTIL_INPUT)).orElse("").toString();

        if (StringUtils.isNotBlank(duration) && StringUtils.isNotBlank(until)) {
            task.setReasonForIncompletion(
                    "Both 'duration' and 'until' specified. Please provide only one input");
            task.setStatus(FAILED_WITH_TERMINAL_ERROR);
            return;
        }

        if (StringUtils.isNotBlank(duration)) {

            Duration timeDuration = parseDuration(duration);
            long waitTimeout = System.currentTimeMillis() + (timeDuration.getSeconds() * 1000);
            task.setWaitTimeout(waitTimeout);

            long seconds = timeDuration.getSeconds();
            task.setCallbackAfterSeconds(seconds);
        } else if (StringUtils.isNotBlank(until)) {
            try {
                Date expiryDate = parseDate(until);
                long timeInMS = expiryDate.getTime();
                long now = System.currentTimeMillis();
                long seconds = (timeInMS - now) / 1000;
                task.setWaitTimeout(timeInMS);
                task.setCallbackAfterSeconds(seconds);

            } catch (ParseException parseException) {
                task.setReasonForIncompletion(
                        "Invalid/Unsupported Wait Until format.  Provided: " + until);
                task.setStatus(FAILED_WITH_TERMINAL_ERROR);
            }
        }
        task.setStatus(IN_PROGRESS);
    }

    @Override
    public void cancel(WorkflowModel workflow, TaskModel task, WorkflowExecutor workflowExecutor) {
        task.setStatus(TaskModel.Status.CANCELED);
    }

    @Override
    public boolean execute(
            WorkflowModel workflow, TaskModel task, WorkflowExecutor workflowExecutor) {
        long timeOut = task.getWaitTimeout();
        if (timeOut == 0) {
            return false;
        }
        if (System.currentTimeMillis() > timeOut) {
            task.setStatus(COMPLETED);
            return true;
        }

        return false;
    }
}
