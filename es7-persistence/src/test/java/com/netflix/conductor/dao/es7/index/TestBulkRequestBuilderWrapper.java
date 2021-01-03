package com.netflix.conductor.dao.es7.index;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.junit.Test;
import org.mockito.Mockito;

public class TestBulkRequestBuilderWrapper {
    BulkRequestBuilder builder = Mockito.mock(BulkRequestBuilder.class);
    BulkRequestBuilderWrapper wrapper =new BulkRequestBuilderWrapper(builder);

    @Test(expected = Exception.class)
    public void testAddNullUpdateRequest() {
        wrapper.add((UpdateRequest) null);
    }

    @Test(expected = Exception.class)
    public void testAddNullIndexRequest() {
        wrapper.add((IndexRequest) null);
    }

    @Test
    public void testBuilderCalls() {
        IndexRequest indexRequest = new IndexRequest();
        UpdateRequest updateRequest = new UpdateRequest();

        wrapper.add(indexRequest);
        wrapper.add(updateRequest);
        wrapper.numberOfActions();
        wrapper.execute();

        Mockito.verify(builder, Mockito.times(1)).add(indexRequest);
        Mockito.verify(builder, Mockito.times(1)).add(updateRequest);
        Mockito.verify(builder, Mockito.times(1)).numberOfActions();
        Mockito.verify(builder, Mockito.times(1)).execute();
    }

}
