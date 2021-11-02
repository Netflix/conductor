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
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
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
	private static final String INSERT_QUERY = "INSERT INTO log4j_logs " +
		"(log_time, logger, level, owner, hostname, fromhost, message, stack, alloc_id, trace_id, span_id) " +
		"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

	private final LinkedBlockingDeque<LogEntry> buffer = new LinkedBlockingDeque<>();
	private final AtomicBoolean initialized = new AtomicBoolean(false);
	private final ScheduledExecutorService execs;
	private HikariDataSource dataSource;
	private final String hostname;
	private final String fromhost;
	private final String allocId;
	private String url;
	private String user;
	private String password;

	public DeluxeAuroraAppender() {
		super();
		hostname = getHostName();
		fromhost = getHostIp();
		allocId = System.getenv("NOMAD_ALLOC_ID");
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
		entry.traceId = (String)event.getMDC("dd.trace_id");
		entry.spanId = (String)event.getMDC("dd.span_id");
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
				poolConfig.setMaximumPoolSize(getIntEnv("aurora_log4j_pool_size",1));
				poolConfig.addDataSourceProperty("ApplicationName", "log4j-" + hostname);
				poolConfig.addDataSourceProperty("cachePrepStmts", "true");
				poolConfig.addDataSourceProperty("prepStmtCacheSize", "250");
				poolConfig.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

				dataSource = new HikariDataSource(poolConfig);
				Runtime.getRuntime().addShutdownHook(new Thread(() -> {
					try {
						this.close();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}));
			}

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
		try (Connection tx = dataSource.getConnection(); PreparedStatement st = tx.prepareStatement(INSERT_QUERY)) {

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
				st.setString(9, allocId);
				st.setString(10, entry.traceId);
				st.setString(11, entry.spanId);
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
		try {
			System.out.println("Closing log4j data source");
			dataSource.close();
		} catch (Exception ignore) {
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


	private int getIntEnv(String name, int defValue) {
		String value = System.getenv(name);
		return StringUtils.isEmpty(value) ? defValue : Integer.parseInt(value);
	}

	private static class LogEntry {
		Timestamp timestamp;
		String level;
		String logger;
		String owner;
		String message;
		String stack;
		String traceId;
		String spanId;
	}
}