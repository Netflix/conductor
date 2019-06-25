package com.netflix.conductor.archiver.job;

import com.zaxxer.hikari.HikariDataSource;

public abstract class AbstractJob {
	HikariDataSource dataSource;

	AbstractJob(HikariDataSource dataSource) {
		this.dataSource = dataSource;
	}

	public abstract void cleanup() throws Exception;
}
