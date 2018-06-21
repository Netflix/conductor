package com.netflix.conductor.dao.es5.index;

import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import javax.inject.Inject;

public class CustomHighLevelRestClient extends RestHighLevelClient{
    public RestClient restClient;

    @Inject
    public CustomHighLevelRestClient(RestClient restClient) {
        super(restClient);
        this.restClient = restClient;
    }


    public RestClient getLowLevelRestClient() {
        return this.restClient;
    }
}
