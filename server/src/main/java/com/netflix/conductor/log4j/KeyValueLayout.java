package com.netflix.conductor.log4j;

import org.apache.log4j.Layout;
import org.apache.log4j.spi.LoggingEvent;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

public class KeyValueLayout extends Layout {
    private DateTimeFormatter dateTime = ISODateTimeFormat.dateTime().withZoneUTC();

    @Override
    public String format(LoggingEvent event) {
        StringBuilder buf = new StringBuilder();
        buf.append("timestamp=").append(currentTime()).append(" ");
        buf.append("severity=").append(event.getLevel().toString().toLowerCase()).append(" ");
        buf.append("logger=").append(event.getLoggerName().toLowerCase()).append(" ");
        buf.append("text=").append(event.getMessage());
        buf.append("\n");
        return buf.toString();
    }

    private String currentTime() {
        return dateTime.print(new DateTime());
    }

    @Override
    public boolean ignoresThrowable() {
        return true;
    }

    @Override
    public void activateOptions() {
    }
}
