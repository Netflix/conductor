package com.netflix.conductor.contribs.correlation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.sun.jersey.api.client.WebResource;

/**
 * Created by beimforz on 12/21/17.
 */
public interface ICorrelator {
    void addIdentifier(String urn);

    void attach(WebResource.Builder builder) throws JsonProcessingException;
}
