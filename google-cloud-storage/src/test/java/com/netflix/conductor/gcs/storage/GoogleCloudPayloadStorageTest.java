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

import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import com.netflix.conductor.common.run.ExternalStorageLocation;
import com.netflix.conductor.common.utils.ExternalPayloadStorage;
import com.netflix.conductor.core.exception.ApplicationException;
import java.time.Duration;

import com.netflix.conductor.gcs.config.GCSProperties;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Build path on external storage. mostly copied from AzureBlobPayloadStorageTest.
 *
 * **/

public class GoogleCloudPayloadStorageTest {

    private GCSProperties properties;

    @Before
    public void setUp() {
        properties = mock(GCSProperties.class);
        when(properties.getProjectId()).thenReturn("fake-project-for-testing");
        when(properties.getBucketName()).thenReturn("conductor-payloads");
        when(properties.getSignedUrlExpirationDuration()).thenReturn(Duration.ofSeconds(5));
        when(properties.getWorkflowInputPath()).thenReturn("workflow/input/");
        when(properties.getWorkflowOutputPath()).thenReturn("workflow/output/");
        when(properties.getTaskInputPath()).thenReturn("task/input");
        when(properties.getTaskOutputPath()).thenReturn("task/output/");
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testMissingBucketName() {
        expectedException.expect(ApplicationException.class);
        when(properties.getBucketName()).thenReturn(null);
        new GoogleCloudPayloadStorage(properties, LocalStorageHelper.getOptions());
    }

    @Test
    public void testGetLocationFixedPath() {
        GoogleCloudPayloadStorage googleCloudPayloadStorage = new GoogleCloudPayloadStorage(properties, LocalStorageHelper.getOptions());
        String path = "somewhere";
        ExternalStorageLocation externalStorageLocation = googleCloudPayloadStorage
            .getLocation(ExternalPayloadStorage.Operation.READ, ExternalPayloadStorage.PayloadType.WORKFLOW_INPUT,
                path);
        assertNotNull(externalStorageLocation);
        assertEquals(path, externalStorageLocation.getPath());
        assertNotNull(externalStorageLocation.getUri());
    }

    private void testGetLocation(GoogleCloudPayloadStorage googleCloudPayloadStorage,
        ExternalPayloadStorage.Operation operation, ExternalPayloadStorage.PayloadType payloadType,
        String expectedPath) {
        ExternalStorageLocation externalStorageLocation = googleCloudPayloadStorage
            .getLocation(operation, payloadType, null);
        assertNotNull(externalStorageLocation);
        assertNotNull(externalStorageLocation.getPath());
        assertTrue(externalStorageLocation.getPath().startsWith(expectedPath));
        assertNotNull(externalStorageLocation.getUri());
        assertTrue(externalStorageLocation.getUri().contains(expectedPath));
    }

    @Test
    public void testGetAllLocations() {
        GoogleCloudPayloadStorage googleCloudPayloadStorage = new GoogleCloudPayloadStorage(properties);

        testGetLocation(googleCloudPayloadStorage, ExternalPayloadStorage.Operation.READ,
            ExternalPayloadStorage.PayloadType.WORKFLOW_INPUT, properties.getWorkflowInputPath());
        testGetLocation(googleCloudPayloadStorage, ExternalPayloadStorage.Operation.READ,
            ExternalPayloadStorage.PayloadType.WORKFLOW_OUTPUT, properties.getWorkflowOutputPath());
        testGetLocation(googleCloudPayloadStorage, ExternalPayloadStorage.Operation.READ,
            ExternalPayloadStorage.PayloadType.TASK_INPUT, properties.getTaskInputPath());
        testGetLocation(googleCloudPayloadStorage, ExternalPayloadStorage.Operation.READ,
            ExternalPayloadStorage.PayloadType.TASK_OUTPUT, properties.getTaskOutputPath());

        testGetLocation(googleCloudPayloadStorage, ExternalPayloadStorage.Operation.WRITE,
            ExternalPayloadStorage.PayloadType.WORKFLOW_INPUT, properties.getWorkflowInputPath());
        testGetLocation(googleCloudPayloadStorage, ExternalPayloadStorage.Operation.WRITE,
            ExternalPayloadStorage.PayloadType.WORKFLOW_OUTPUT, properties.getWorkflowOutputPath());
        testGetLocation(googleCloudPayloadStorage, ExternalPayloadStorage.Operation.WRITE,
            ExternalPayloadStorage.PayloadType.TASK_INPUT, properties.getTaskInputPath());
        testGetLocation(googleCloudPayloadStorage, ExternalPayloadStorage.Operation.WRITE,
            ExternalPayloadStorage.PayloadType.TASK_OUTPUT, properties.getTaskOutputPath());
    }
}
