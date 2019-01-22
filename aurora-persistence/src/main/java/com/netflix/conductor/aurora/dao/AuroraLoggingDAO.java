package com.netflix.conductor.aurora.dao;

import com.netflix.conductor.dao.LoggingDAO;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.log4j.Level;
import org.apache.log4j.NDC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;

@Singleton
public class AuroraLoggingDAO implements LoggingDAO {
    private static Logger logger = LoggerFactory.getLogger(AuroraLoggingDAO.class);

    private static final String INSERT_QUERY = "INSERT INTO logs (log_time, severity, hostname, fromhost, logger, owner, message) " +
            " VALUES (?, ?, ?, ?, ?, ?, ?)";
    private HikariDataSource dataSource;
    private String hostname;
    private String fromhost;

    @Inject
    public AuroraLoggingDAO(HikariDataSource dataSource) {
        this.dataSource = dataSource;
        hostname = getHostName();
        fromhost = getHostIp();
        try(Connection tx = dataSource.getConnection()) {
            PreparedStatement st = tx.prepareStatement("");

        } catch (Exception ex) {

        }
    }

    @Override
    public void info(Class clazz, String msg) {
        log(clazz, Level.INFO, msg);
    }

    @Override
    public void info(Class clazz, String format, Object... arguments) {
        log(clazz, Level.INFO, String.format(format, arguments));
    }

    @Override
    public void debug(Class clazz, String msg) {
        log(clazz, Level.DEBUG, msg);
    }

    @Override
    public void debug(Class clazz, String format, Object... arguments) {
        log(clazz, Level.DEBUG, String.format(format, arguments));
    }

    private void log(Class clazz, Level level, String message) {
        try(Connection tx = dataSource.getConnection()) {
            PreparedStatement st = tx.prepareStatement(INSERT_QUERY);
            st.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
            st.setString(2, level.toString());
            st.setString(3, hostname);
            st.setString(4, fromhost);
            st.setString(5, clazz.getName());
            st.setString(6, NDC.get());
            st.setString(7, message);
            st.execute();
        } catch (Exception ex) {
            logger.error("log failed with " + ex.getMessage(), ex);
        }
    }

    private String getHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return "unknown";
        }
    }

    private String getHostIp() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return "unknown";
        }
    }
}
