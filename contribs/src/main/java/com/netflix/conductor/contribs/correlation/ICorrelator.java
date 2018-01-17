package com.netflix.conductor.contribs.correlation;

import com.sun.jersey.api.core.HttpContext;
import sun.net.www.http.HttpClient;


/**
 * Created by beimforz on 12/21/17.
 */
public interface ICorrelator {
    void init(HttpContext context);

    void addIdentifier(String urn);

    void attach(HttpClient client);
}
