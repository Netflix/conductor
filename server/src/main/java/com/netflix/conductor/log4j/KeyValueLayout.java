package com.netflix.conductor.log4j;

import org.apache.log4j.Layout;
import org.apache.log4j.spi.LoggingEvent;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class KeyValueLayout extends Layout {
    private DateTimeFormatter dateTime = ISODateTimeFormat.dateTime().withZoneUTC();
    private String hostname;
    private String fromhost;
    private String allocId;

    public KeyValueLayout() {
        hostname = getHostName();
        fromhost = getHostIp();
        allocId = System.getenv("NOMAD_ALLOC_ID");
    }

    @Override
    public String format(LoggingEvent event) {
        StringBuilder buf = new StringBuilder();
        buf.append("timestamp=").append(timestamp()).append(" ");
        buf.append("service=").append("conductor").append(" ");
        buf.append("hostname=").append(hostname).append(" ");
        buf.append("fromhost=").append(fromhost).append(" ");
        buf.append("log-level=").append(event.getLevel().toString().toLowerCase()).append(" ");
        buf.append("severity=").append(event.getLevel().toString().toLowerCase()).append(" ");
        buf.append("logger=").append(event.getLoggerName()).append(" ");
        buf.append("owner=").append(event.getNDC()).append(" ");
        buf.append("allocId=").append(allocId).append(" ");
        buf.append("text=").append("\"").append(normalizeMessage(event.getMessage())).append("\"");
        if (event.getThrowableInformation() != null
                && event.getThrowableInformation().getThrowable() != null) {
            Throwable throwable = event.getThrowableInformation().getThrowable();

            StringWriter sw = new StringWriter();
            throwable.printStackTrace(new PrintWriter(sw));

            buf.append(" stackTrace=").append("\"").append(normalizeMessage(sw.toString())).append("\"");
        }
        buf.append("\n");

        return buf.toString();
    }

    private String normalizeMessage(Object message) {
        String response = "";
        if (message != null) {
            response = message.toString();
            if (response.contains("\n")) {
                response = response.replace("\n", "");
            }
            if (response.contains("\t")) {
                response = response.replace("\t", " ");
            }
            if (response.contains("\"")) {
                response = response.replace("\"", "'");
            }
        }

        return response;
    }

    private String timestamp() {
        return dateTime.print(new DateTime());
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

    @Override
    public boolean ignoresThrowable() {
        return false; // See ONECOND-708: https://jira.d3nw.com/browse/ONECOND-708
    }

    @Override
    public void activateOptions() {
    }
}
