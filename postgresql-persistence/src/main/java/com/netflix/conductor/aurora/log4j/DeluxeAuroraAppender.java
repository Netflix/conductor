/**
 * Copyright 2016 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 *
 */
package com.netflix.conductor.aurora.log4j;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Oleksiy Lysak
 */
public class DeluxeAuroraAppender extends AppenderSkeleton {
	private static final String CREATE_INDEX = "create index log4j_logs_log_time_idx on log4j_logs (log_time)";

	private static final String CREATE_TABLE = "create table log4j_logs(\n" +
		"  log_time timestamp,\n" +
		"  logger varchar,\n" +
		"  level varchar,\n" +
		"  owner varchar,\n" +
		"  hostname varchar,\n" +
		"  fromhost varchar,\n" +
		"  message varchar,\n" +
		"  stack varchar\n" +
		")";

	private static final String INSERT_QUERY = "INSERT INTO log4j_logs " +
		"(log_time, logger, level, owner, hostname, fromhost, message, stack) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

	private LinkedBlockingDeque<LogEntry> buffer = new LinkedBlockingDeque<>();
	private AtomicBoolean initialized = new AtomicBoolean(false);
	private ScheduledExecutorService execs;
	private DataSource dataSource;
	private String hostname;
	private String fromhost;
	private String url;
	private String user;
	private String password;

	public DeluxeAuroraAppender() {
		super();
		hostname = getHostName();
		fromhost = getHostIp();
		execs = Executors.newScheduledThreadPool(1);
		execs.scheduleWithFixedDelay(this::flush, 500, 100, TimeUnit.MILLISECONDS);
	}

	public void append(LoggingEvent event) {
		LogEntry entry = new LogEntry();
		entry.timestamp = new Timestamp(System.currentTimeMillis());
		entry.level = event.getLevel().toString().toLowerCase();
		entry.logger = event.getLoggerName();
		entry.owner = event.getNDC();
		entry.message = normalizeMessage(event.getRenderedMessage());
		if (event.getThrowableInformation() != null
			&& event.getThrowableInformation().getThrowable() != null) {
			Throwable throwable = event.getThrowableInformation().getThrowable();
			entry.stack = throwable2String(throwable);
		}
		buffer.add(entry);
	}

	public void init() {
		try {
			if (dataSource == null) {
				HikariConfig poolConfig = new HikariConfig();
				poolConfig.setJdbcUrl(url);
				poolConfig.setUsername(user);
				poolConfig.setPassword(password);
				poolConfig.setAutoCommit(true);
				poolConfig.setPoolName("log4j");
				poolConfig.addDataSourceProperty("ApplicationName", "log4j");
				poolConfig.addDataSourceProperty("cachePrepStmts", "true");
				poolConfig.addDataSourceProperty("prepStmtCacheSize", "250");
				poolConfig.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

				dataSource = new HikariDataSource(poolConfig);
			}

			execute(CREATE_TABLE);
			execute(CREATE_INDEX);

			initialized.set(true);
		} catch (Exception ex) {
			System.out.println("DeluxeAuroraAppender.init failed " + ex.getMessage() + ", stack=" + throwable2String(ex));
		}
	}

	private void flush() {
		if (buffer.isEmpty()) {
			return;
		}
		if (!initialized.get()) {
			init();
		}
		try (Connection tx = dataSource.getConnection(); PreparedStatement st = tx.prepareStatement(INSERT_QUERY);) {

			LogEntry entry = buffer.poll();
			while (entry != null) {
				st.setTimestamp(1, entry.timestamp);
				st.setString(2, entry.logger);
				st.setString(3, entry.level);
				st.setString(4, entry.owner);
				st.setString(5, hostname);
				st.setString(6, fromhost);
				st.setString(7, entry.message);
				st.setString(8, entry.stack);
				st.execute();

				// Get the next
				entry = buffer.take();
			}
		} catch (Exception ex) {
			System.out.println("DeluxeAuroraAppender.flush failed " + ex.getMessage() + ", stack=" + throwable2String(ex));
		}
	}

	public void close() {
		execs.shutdown();
		try {
			execs.awaitTermination(5, TimeUnit.SECONDS);
		} catch (InterruptedException ignore) {
		}
		this.closed = true;
	}

	public boolean requiresLayout() {
		return false;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getUser() {
		return user;
	}

	public String getUrl() {
		return url;
	}

	public String getPassword() {
		return password;
	}

	private void execute(String ddl) {
		try (Connection tx = dataSource.getConnection(); CallableStatement st = tx.prepareCall(ddl);) {
			st.execute();
		} catch (Exception ex) {
			if (!ex.getMessage().contains("already exists")) {
				System.out.println("DeluxeAuroraAppender.execute failed " + ex.getMessage() + ", stack=" + throwable2String(ex));
			}
		}
	}

	private String normalizeMessage(String message) {
		String response = "";
		if (message != null) {
			response = message;
			if (response.contains("\n")) {
				response = response.replace("\n", "");
			}
			if (response.contains("\t")) {
				response = response.replace("\t", " ");
			}
		}

		return response;
	}

	private String throwable2String(Throwable throwable) {
		StringWriter sw = new StringWriter();
		throwable.printStackTrace(new PrintWriter(sw));
		return normalizeMessage(sw.toString());
	}

	private String getHostName() {
		try {
			return InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			return "unknown";
		}
	}

	private String getHostIp() {
		try {
			return InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			return "unknown";
		}
	}

	private static class LogEntry {
		Timestamp timestamp;
		String level;
		String logger;
		String owner;
		String message;
		String stack;
	}
}