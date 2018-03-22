package com.netflix.conductor.contribs.correlation;

import com.fasterxml.jackson.core.JsonProcessingException;

import java.util.Map;

/**
 * Created by beimforz on 12/21/17.
 */
public interface ICorrelator {
    void addIdentifier(String urn);

    void attach(Map<String, Object> headers) throws JsonProcessingException;
}
