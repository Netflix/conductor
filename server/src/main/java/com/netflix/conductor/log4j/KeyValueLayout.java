package com.netflix.conductor.log4j;

import org.apache.log4j.Layout;
import org.apache.log4j.spi.LoggingEvent;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class KeyValueLayout extends Layout {
    private DateTimeFormatter dateTime = ISODateTimeFormat.dateTime().withZoneUTC();

    @Override
    public String format(LoggingEvent event) {
        StringBuilder buf = new StringBuilder();
        buf.append("timestamp=").append(timestamp()).append(" ");
        buf.append("service=").append("conductor").append(" ");
        buf.append("hostname=").append(hostname()).append(" ");
        buf.append("fromhost=").append(fromhost()).append(" ");
        buf.append("log-level=").append(event.getLevel().toString().toLowerCase()).append(" ");
        buf.append("severity=").append(event.getLevel().toString().toLowerCase()).append(" ");
        buf.append("logger=").append(event.getLoggerName().toLowerCase()).append(" ");
        buf.append("text=").append(event.getMessage());
        buf.append("\n");
        return buf.toString();
    }

    private String timestamp() {
        return dateTime.print(new DateTime());
    }

    private String hostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return "unknown";
        }
    }

    private String fromhost() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return "unknown";
        }
    }

    @Override
    public boolean ignoresThrowable() {
        return true;
    }

    @Override
    public void activateOptions() {
    }
}
