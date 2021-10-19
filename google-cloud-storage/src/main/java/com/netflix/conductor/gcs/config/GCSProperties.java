/*
 * Copyright 2020 Netflix, Inc.
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
package com.netflix.conductor.gcs.config;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.convert.DurationUnit;

@ConfigurationProperties("conductor.external-payload-storage.gcs")
public class GCSProperties {

    /**
     * The path to the JSON file that contains your service account key
     */
    private String googleApplicationCredentialsPath = null;

    /**
     * The ID of your GCP project
     */
    private String projectId = null;

    /**
     * The name of the bucket where the payloads will be stored
     */
    private String bucketName = "conductor-payloads";

    /**
     * The time for which the shared access signature is valid
     */
    @DurationUnit(ChronoUnit.SECONDS)
    private Duration signedUrlExpirationDuration = Duration.ofSeconds(5);

    /**
     * The path at which the workflow inputs will be stored
     */
    private String workflowInputPath = "workflow/input/";

    /**
     * The path at which the workflow outputs will be stored
     */
    private String workflowOutputPath = "workflow/output/";

    /**
     * The path at which the task inputs will be stored
     */
    private String taskInputPath = "task/input/";

    /**
     * The path at which the task outputs will be stored
     */
    private String taskOutputPath = "task/output/";

    public String getProjectId() {
        return projectId;
    }

    public String getGoogleApplicationCredentialsPath() {
        return googleApplicationCredentialsPath;
    }

    public void setGoogleApplicationCredentialsPath(String googleApplicationCredentialsPath) {
        googleApplicationCredentialsPath = googleApplicationCredentialsPath;
    }

    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String containerName) {
        this.bucketName = containerName;
    }

    public void setProjectId(String projectId) {
        this.projectId = projectId;
    }

    public Duration getSignedUrlExpirationDuration() {
        return signedUrlExpirationDuration;
    }

    public void setSignedUrlExpirationDuration(Duration signedUrlExpirationDuration) {
        this.signedUrlExpirationDuration = signedUrlExpirationDuration;
    }

    public String getWorkflowInputPath() {
        return workflowInputPath;
    }

    public void setWorkflowInputPath(String workflowInputPath) {
        this.workflowInputPath = workflowInputPath;
    }

    public String getWorkflowOutputPath() {
        return workflowOutputPath;
    }

    public void setWorkflowOutputPath(String workflowOutputPath) {
        this.workflowOutputPath = workflowOutputPath;
    }

    public String getTaskInputPath() {
        return taskInputPath;
    }

    public void setTaskInputPath(String taskInputPath) {
        this.taskInputPath = taskInputPath;
    }

    public String getTaskOutputPath() {
        return taskOutputPath;
    }

    public void setTaskOutputPath(String taskOutputPath) {
        this.taskOutputPath = taskOutputPath;
    }
}
