package com.netflix.conductor.dao.es5.index;

import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

public class CustomHighLevelRestClient extends RestHighLevelClient{
    public RestClient restClient;

    public CustomHighLevelRestClient(RestClient restClient) {
        super(restClient);
        this.restClient = restClient;
    }


    public RestClient getLowLevelRestClient() {
        return this.restClient;
    }
}
