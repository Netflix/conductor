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
package com.netflix.conductor.gcs.storage;


import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.*;
import com.google.common.io.ByteStreams;
import com.netflix.conductor.common.run.ExternalStorageLocation;
import com.netflix.conductor.common.utils.ExternalPayloadStorage;
import com.netflix.conductor.core.exception.ApplicationException;
import com.netflix.conductor.core.utils.IDGenerator;

import java.io.*;
import java.util.concurrent.TimeUnit;

import com.netflix.conductor.gcs.config.GCSProperties;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * An implementation of {@link ExternalPayloadStorage} using Google Cloud Storage for storing large JSON payload data.
 *
 * @see <a href="https://github.com/GoogleCloudPlatform/java-docs-samples">Google Cloud Storage SDK</a>
 */

public class GoogleCloudPayloadStorage implements ExternalPayloadStorage {

    private static final Logger LOGGER = LoggerFactory.getLogger(GoogleCloudPayloadStorage.class);
    private static final String CONTENT_TYPE = "application/json";

    private final String workflowInputPath;
    private final String workflowOutputPath;
    private final String taskInputPath;
    private final String taskOutputPath;

    private final Storage storage;
    private final String bucketName;
    private final long expirationSec;


    public GoogleCloudPayloadStorage(GCSProperties properties) {
        this(properties, null);
    }

    public GoogleCloudPayloadStorage(GCSProperties properties, @Nullable StorageOptions storageOptions) {
        StorageOptions.Builder storageOptionsBuilder = StorageOptions.newBuilder();
        // workaround for testing...
        if (storageOptions != null){
            storageOptionsBuilder = storageOptions.toBuilder();
        }

        workflowInputPath = properties.getWorkflowInputPath();
        workflowOutputPath = properties.getWorkflowOutputPath();
        taskInputPath = properties.getTaskInputPath();
        taskOutputPath = properties.getTaskOutputPath();
        expirationSec = properties.getSignedUrlExpirationDuration().getSeconds();
        String projectId = properties.getProjectId();
        String googleApplicationCredentialsPath = properties.getGoogleApplicationCredentialsPath();
        String bucketName = properties.getBucketName();


        if (projectId != null) {
            storageOptionsBuilder.setProjectId(projectId);
        }
        if (bucketName != null) {
            this.bucketName = bucketName;
        }else {
            String msg = "GCS bucket name missing!";
            LOGGER.error(msg);
            throw new ApplicationException(ApplicationException.Code.BACKEND_ERROR, msg);
        }
        if (googleApplicationCredentialsPath != null) {
            try{
                storageOptionsBuilder.setCredentials(ServiceAccountCredentials.fromStream(new FileInputStream(googleApplicationCredentialsPath)));
            }catch (FileNotFoundException e) {
                String msg = "Could not load googleApplicationCredentialsPath";
                LOGGER.error(msg);
                throw new ApplicationException(ApplicationException.Code.BACKEND_ERROR, msg);
            } catch (IOException e) {
                String msg = "Could not load googleApplicationCredentialsPath";
                LOGGER.error(msg);
                throw new ApplicationException(ApplicationException.Code.BACKEND_ERROR, msg);
            }

        } else {
            String msg = "Could not load googleApplicationCredentialsPath";
            LOGGER.error(msg);
            throw new ApplicationException(ApplicationException.Code.BACKEND_ERROR, msg);
        }
        storage = storageOptionsBuilder.build().getService();
    }

    /**
     * @param operation   the type of {@link Operation} to be performed
     * @param payloadType the {@link PayloadType} that is being accessed
     * @return a {@link ExternalStorageLocation} object which contains the pre-signed URL and the azure blob name for
     * the json payload
     */
    @Override
    public ExternalStorageLocation getLocation(Operation operation, PayloadType payloadType, String path) {
        try {
            ExternalStorageLocation externalStorageLocation = new ExternalStorageLocation();

            String objectName;
            if (StringUtils.isNotBlank(path)) {
                objectName = path;
            } else {
                objectName = getObjectKey(payloadType);
            }
            externalStorageLocation.setPath(objectName);

            Blob blob = storage.get(BlobId.of(bucketName, objectName));
            String blobUrl = blob.signUrl(expirationSec, TimeUnit.SECONDS).toString();

            externalStorageLocation.setUri(blobUrl);
            return externalStorageLocation;
        } catch (Exception e) {
            String msg = "Error communicating with Google";
            LOGGER.error(msg, e);
            throw new ApplicationException(ApplicationException.Code.BACKEND_ERROR, msg, e);
        }
    }

    /**
     * Uploads the payload to the given GCS object name. It is expected that the caller retrieves the object name using
     * {@link #getLocation(Operation, PayloadType, String)} before making this call.
     *
     * @param path        the name of the object to be uploaded
     * @param payload     an {@link InputStream} containing the json payload which is to be uploaded
     * @param payloadSize the size of the json payload in bytes
     */
    @Override
    public void upload(String path, InputStream payload, long payloadSize) {
        try {
            String objectName = path;
            BlobId blobId = BlobId.of(bucketName, objectName);
            BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType(CONTENT_TYPE).build();
            storage.create(blobInfo, ByteStreams.toByteArray(payload));

        } catch (UncheckedIOException | IOException e) {
            String msg = "Error communicating with Google";
            LOGGER.error(msg, e);
            throw new ApplicationException(ApplicationException.Code.BACKEND_ERROR, msg, e);
        }
    }

    /**
     * Downloads the payload stored in an azure blob.
     *
     * @param path the path of the object
     * @return an input stream containing the contents of the object Caller is expected to close the input stream.
     */
    @Override
    public InputStream download(String path) {
        try {
            String objectName = path;
            Blob blob = storage.get(BlobId.of(bucketName, objectName));
            return new ByteArrayInputStream(blob.getContent());
        } catch (UncheckedIOException | NullPointerException e) {
            String msg = "Error communicating with Google";
            LOGGER.error(msg, e);
            throw new ApplicationException(ApplicationException.Code.BACKEND_ERROR, msg, e);
        }
    }

    /**
     * Build path on external storage. Copied from S3PayloadStorage.
     *
     * @param payloadType the {@link PayloadType} which will determine the base path of the object
     * @return External Storage path
     */
    private String getObjectKey(PayloadType payloadType) {
        StringBuilder stringBuilder = new StringBuilder();
        switch (payloadType) {
            case WORKFLOW_INPUT:
                stringBuilder.append(workflowInputPath);
                break;
            case WORKFLOW_OUTPUT:
                stringBuilder.append(workflowOutputPath);
                break;
            case TASK_INPUT:
                stringBuilder.append(taskInputPath);
                break;
            case TASK_OUTPUT:
                stringBuilder.append(taskOutputPath);
                break;
        }
        stringBuilder.append(IDGenerator.generate()).append(".json");
        return stringBuilder.toString();
    }
}
