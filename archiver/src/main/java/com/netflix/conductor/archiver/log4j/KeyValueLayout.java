package com.netflix.conductor.archiver.log4j;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.AbstractStringLayout;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;

@Plugin(name = "KeyValueLayout",
        category = "Core",
        elementType = "layout",
        printObject = true
)
public class KeyValueLayout extends AbstractStringLayout {
    private DateTimeFormatter dateTime = ISODateTimeFormat.dateTime().withZoneUTC();
    private String hostname;
    private String fromhost;

    @PluginFactory
    public static KeyValueLayout createLayout() {
        return new KeyValueLayout(Charset.forName("UTF-8"));
    }

    private KeyValueLayout(Charset charset) {
        super(charset);
        hostname = getHostName();
        fromhost = getHostIp();
    }

    @Override
    public String toSerializable(LogEvent event) {
        StringBuilder buf = new StringBuilder();
        buf.append("timestamp=").append(timestamp()).append(" ");
        buf.append("service=").append("conductor-archiver").append(" ");
        buf.append("hostname=").append(hostname).append(" ");
        buf.append("fromhost=").append(fromhost).append(" ");
        buf.append("log-level=").append(event.getLevel().toString().toLowerCase()).append(" ");
        buf.append("severity=").append(event.getLevel().toString().toLowerCase()).append(" ");
        buf.append("logger=").append(event.getLoggerName()).append(" ");
        buf.append("text=").append("\"").append(normalizeMessage(event.getMessage().getFormattedMessage())).append("\"");
        if (event.getThrown() != null) {
            StringWriter errors = new StringWriter();
            event.getThrown().printStackTrace(new PrintWriter(errors));
            buf.append(" stackTrace=").append("\"").append(normalizeMessage(errors.toString())).append("\"");
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
}
