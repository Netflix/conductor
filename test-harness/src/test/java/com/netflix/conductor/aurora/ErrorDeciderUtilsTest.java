package com.netflix.conductor.aurora;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class ErrorDeciderUtilsTest {

    @Test
    public void isKnownError() {
        String metaError =  "Cannot read property get_prefetched_source_file of undefined";

        List<String> workflowErrors = Arrays.asList(
                "exception in analyze task: Cannot read property media_analyze of undefined",
                "exception in analyze task: Cannot read property media_analyze of undefined"
        );

        workflowErrors.forEach(workflowError ->{
            ErrorDeciderUtils.isKnownError(workflowError, metaError);
            Assert.assertTrue(ErrorDeciderUtils.isKnownError(workflowError, metaError));
        });
    }

    @Test
    public void isUnknownError() {
        String logError =  "Cannot read property get_prefetched_source_file of undefined";

        List<String> workflowErrors = Arrays.asList(
                "virtual pipeline mergemap has partial multi track filters, not supported",
                "Error while filtering: Cannot allocate memory: stderr: ffmpeg version"
        );

        workflowErrors.forEach(workflowError ->{
            ErrorDeciderUtils.isKnownError(workflowError, logError);
            Assert.assertTrue(ErrorDeciderUtils.isKnownError(workflowError, logError));
        });
    }
}