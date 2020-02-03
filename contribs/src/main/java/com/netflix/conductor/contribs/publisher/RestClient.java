package com.netflix.conductor.contribs.publisher;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(RestClient.class);
    //private static final String URL = "http://bullwinkle.default:7979/v1/workflow";
    private static final String URL = "http://172.17.2.30:7979/v1";
    private Client client = Client.create();

    public Client getClient(String notificationType) {
        return client;
    }

    public String createUrl(String notificationType) {
        return URL + "/" + notificationType;
    }

    public void post(String uri, String input) {
        LOGGER.info("URL: " + uri);
        LOGGER.info("Input: " + input);
        try {
            WebResource webResource = client.resource(uri);
            ClientResponse response = webResource.type("application/json")
                    .post(ClientResponse.class, input);
            if (response.getStatus() != 200) {
                throw new RuntimeException("Failed : HTTP error code : "
                        + response.getStatus());
            }
            LOGGER.info("Record sent with Status " + response.getEntity(String.class));
        }
        catch (Exception e) {
            LOGGER.info("#####3" + e.toString());
            e.printStackTrace();
        }
    }

    public void get(String uri) {
        LOGGER.info("URL: " + uri);
        try {
            LOGGER.info("#####1");
            WebResource webResource = client.resource(uri);
            ClientResponse response = webResource.type("application/json")
                    .get(ClientResponse.class);
            LOGGER.info("#####2");
            LOGGER.info(response.toString());
            if (response.getStatus() != 200) {
                LOGGER.info("Failed : HTTP error code : "
                        + response.getStatus());
                throw new RuntimeException("Failed : HTTP error code : "
                        + response.getStatus());
            }
            LOGGER.info("Record sent with Status " + response.getEntity(String.class));
        }
        catch (Exception e) {
            LOGGER.info("#####3" + e.toString());
            e.printStackTrace();
        }
    }
}
