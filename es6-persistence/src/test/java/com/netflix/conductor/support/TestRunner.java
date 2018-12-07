package com.netflix.conductor.support;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.conductor.dao.es6.index.ElasticSearchDAOV6;
import com.netflix.conductor.elasticsearch.EmbeddedElasticSearch;
import com.netflix.conductor.elasticsearch.EmbeddedElasticSearchProvider;
import com.netflix.conductor.elasticsearch.es6.ElasticSearchV6Module;
import org.junit.runners.BlockJUnit4ClassRunner;

public class TestRunner extends BlockJUnit4ClassRunner {

    private Injector injector;

    public TestRunner(Class<?> klass) throws Exception {
        super(klass);
        injector = Guice.createInjector(new ElasticSearchV6Module());
        startEmbeddedElasticSearch();
    }

    private void startEmbeddedElasticSearch() {
        try {
            EmbeddedElasticSearch search = injector.getInstance(EmbeddedElasticSearchProvider.class).get().get();
            search.start();

            injector.getInstance(ElasticSearchDAOV6.class).setup();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    protected Object createTest() throws Exception {
        Object test = super.createTest();
        injector.injectMembers(test);
        return test;
    }

}
