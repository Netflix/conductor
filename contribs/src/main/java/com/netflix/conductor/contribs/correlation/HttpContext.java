package com.netflix.conductor.contribs.correlation;

import com.sun.jersey.api.core.HttpRequestContext;
import com.sun.jersey.api.core.HttpResponseContext;
import javax.servlet.http.HttpSession;

/**
 * Created by beimforz on 1/9/2018.
 */
public abstract class HttpContext implements com.sun.jersey.api.core.HttpContext{

    private HttpRequestContext request;
    private HttpResponseContext response;
    private HttpSession session;

    public HttpRequestContext getRequest() {
        return request;
    }

    public void setRequest(HttpRequestContext request) {
        this.request = request;
    }

    public HttpResponseContext getResponse() {
        return response;
    }

    public void setResponse(HttpResponseContext response) {
        this.response = response;
    }


    public HttpSession getSession() {
        return session;
    }

    public void setSession(HttpSession session) {
        this.session = session;
    }
}
