package com.bydeluxe.es2pg.migration;

import org.elasticsearch.search.SearchHit;

@FunctionalInterface
public interface SearchHitHandler {
	void apply(SearchHit hit);
}
