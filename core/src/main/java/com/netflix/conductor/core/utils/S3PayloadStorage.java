/*
 * Copyright 2018 Netflix, Inc.
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

import com.amazonaws.HttpMethod;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.netflix.conductor.common.run.ExternalStorageLocation;
import com.netflix.conductor.common.utils.ExternalPayloadStorage;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.execution.ApplicationException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.Date;

/**
 * An implementation of {@link ExternalPayloadStorage} using AWS S3 for storing large JSON payload data.
 * The S3 client assumes that access to S3 is configured on the instance.
 * @see <a href="https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/index.html?com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html">DefaultAWSCredentialsProviderChain</a>
 */
@Singleton
public class S3PayloadStorage implements ExternalPayloadStorage {
    private static final Logger logger = LoggerFactory.getLogger(S3PayloadStorage.class);
    private static final String CONTENT_TYPE = "application/json";

    private static final String DEFAULT_BUCKET = "conductor_payloads";
    private static final int DEFAULT_SIGNED_URL_EXPIRATION_SECONDS = 5;
    private static final String DEFAULT_REGION = "us-east-1";

    private final AmazonS3 s3Client;
    private final String bucketName;
    private final int expirationSec;

    @Inject
    public S3PayloadStorage(Configuration config) {
        bucketName = config.getProperty("workflow.external.payload.storage.s3.bucket", DEFAULT_BUCKET);
        expirationSec = config.getIntProperty("workflow.external.payload.storage.s3.signedurlexpirationseconds", DEFAULT_SIGNED_URL_EXPIRATION_SECONDS);

        String region = config.getProperty("workflow.external.payload.storage.s3.region", DEFAULT_REGION);
        s3Client = AmazonS3ClientBuilder.standard().withRegion(region).build();
    }

    /**
     * @param operation   the type of {@link Operation} to be performed
     * @param payloadType the {@link PayloadType} that is being accessed
     * @return a {@link ExternalStorageLocation} object which contains the pre-signed URL and the s3 object key for the json payload
     */
    @Override
    public ExternalStorageLocation getLocation(Operation operation, PayloadType payloadType, String path) {
        try {
            ExternalStorageLocation externalStorageLocation = new ExternalStorageLocation();

            Date expiration = new Date();
            long expTimeMillis = expiration.getTime() + 1000 * expirationSec;
            expiration.setTime(expTimeMillis);

            HttpMethod httpMethod = HttpMethod.GET;
            if (operation == Operation.WRITE) {
                httpMethod = HttpMethod.PUT;
            }

            String objectKey;
            if (StringUtils.isNotBlank(path)) {
                objectKey = path;
            } else {
                objectKey = getObjectKey(payloadType);
            }
            externalStorageLocation.setPath(objectKey);

            GeneratePresignedUrlRequest generatePresignedUrlRequest = new GeneratePresignedUrlRequest(bucketName, objectKey)
                    .withMethod(httpMethod)
                    .withExpiration(expiration);

            externalStorageLocation.setUri(s3Client.generatePresignedUrl(generatePresignedUrlRequest).toURI().toASCIIString());
            return externalStorageLocation;
        } catch (SdkClientException e) {
            String msg = "Error communicating with S3";
            logger.error(msg, e);
            throw new ApplicationException(ApplicationException.Code.BACKEND_ERROR, msg, e);
        } catch (URISyntaxException e) {
            String msg = "Invalid URI Syntax";
            logger.error(msg, e);
            throw new ApplicationException(ApplicationException.Code.INTERNAL_ERROR, msg, e);
        }
    }

    /**
     * Uploads the payload to the given s3 object key.
     * It is expected that the caller retrieves the object key using {@link #getLocation(Operation, PayloadType, String)} before making this call.
     *
     * @param path        the s3 key of the object to be uploaded
     * @param payload     an {@link InputStream} containing the json payload which is to be uploaded
     * @param payloadSize the size of the json payload in bytes
     */
    @Override
    public void upload(String path, InputStream payload, long payloadSize) {
        try {
            ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.setContentType(CONTENT_TYPE);
            objectMetadata.setContentLength(payloadSize);
            PutObjectRequest request = new PutObjectRequest(bucketName, path, payload, objectMetadata);
            s3Client.putObject(request);
        } catch (SdkClientException e) {
            String msg = "Error communicating with S3";
            logger.error(msg, e);
            throw new ApplicationException(ApplicationException.Code.BACKEND_ERROR, msg, e);
        }
    }

    /**
     * Downloads the payload stored in the s3 object.
     *
     * @param path the S3 key of the object
     * @return an input stream containing the contents of the object
     * Caller is expected to close the input stream.
     */
    @Override
    public InputStream download(String path) {
        try {
            S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketName, path));
            return s3Object.getObjectContent();
        } catch (SdkClientException e) {
            String msg = "Error communicating with S3";
            logger.error(msg, e);
            throw new ApplicationException(ApplicationException.Code.BACKEND_ERROR, msg, e);
        }
    }

    private String getObjectKey(PayloadType payloadType) {
        StringBuilder stringBuilder = new StringBuilder();
        switch (payloadType) {
            case WORKFLOW_INPUT:
                stringBuilder.append("workflow/input/");
                break;
            case WORKFLOW_OUTPUT:
                stringBuilder.append("workflow/output/");
                break;
            case TASK_INPUT:
                stringBuilder.append("task/input/");
                break;
            case TASK_OUTPUT:
                stringBuilder.append("task/output/");
                break;
        }
        stringBuilder.append(IDGenerator.generate()).append(".json");
        return stringBuilder.toString();
    }
}
