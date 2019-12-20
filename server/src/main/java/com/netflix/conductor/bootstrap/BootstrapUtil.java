package com.netflix.conductor.bootstrap;

import com.netflix.conductor.dao.IndexDAO;
import com.netflix.conductor.elasticsearch.EmbeddedElasticSearch;
import com.netflix.conductor.grpc.server.GRPCServer;
import org.apache.log4j.PropertyConfigurator;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Optional;
import java.util.Properties;

public class BootstrapUtil {
    public static void startEmbeddedElasticServer(Optional<EmbeddedElasticSearch> embeddedSearchInstance) {

        final int EMBEDDED_ES_INIT_TIME = 5000;

        if (embeddedSearchInstance.isPresent()) {
            try {
                embeddedSearchInstance.get().start();
                /*
                 * Elasticsearch embedded instance does not notify when it is up and ready to accept incoming requests.
                 * A possible solution for reading and writing into the index is to wait a specific amount of time.
                 */
                Thread.sleep(EMBEDDED_ES_INIT_TIME);
            } catch (Exception ioe) {
                System.out.println("Error starting Embedded ElasticSearch");
                ioe.printStackTrace(System.err);
                System.exit(3);
            }
        }
    }


    public static void setupIndex(IndexDAO indexDAO) {
        try {
            indexDAO.setup();
        } catch (Exception e) {
            System.out.println("Error setting up elasticsearch index");
            e.printStackTrace(System.err);
            System.exit(3);
        }
    }

    public static void startGRPCServer(Optional<GRPCServer> grpcServer) {
        grpcServer.ifPresent(server -> {
            try {
                server.start();
            } catch (IOException ioe)
            {
                System.out.println("Error starting GRPC server");
                ioe.printStackTrace(System.err);
                System.exit(3);
            }
        });
    }

    public static void loadConfigFile(String propertyFile) throws IOException {
        if (propertyFile == null) return;
        System.out.println("Using config file: " + propertyFile);
        Properties props = new Properties(System.getProperties());
        props.load(new FileInputStream(propertyFile));
        System.setProperties(props);
    }

    public static void loadLog4jConfig(String log4jConfigFile) throws FileNotFoundException {
            if (log4jConfigFile != null) {
                PropertyConfigurator.configure(new FileInputStream(log4jConfigFile));
            }
    }

}
