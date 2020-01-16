package com.netflix.conductor.mysql;

import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.secretsmanager.model.DecryptionFailureException;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import com.amazonaws.services.secretsmanager.model.InternalServiceErrorException;
import com.amazonaws.services.secretsmanager.model.InvalidParameterException;
import com.amazonaws.services.secretsmanager.model.InvalidRequestException;
import com.amazonaws.services.secretsmanager.model.ResourceNotFoundException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.core.config.SystemPropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Base64.getDecoder;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

public class AWSSecret extends SystemPropertiesConfiguration {
    private static final Logger logger = LoggerFactory.getLogger(AWSSecret.class);
    private static final String CONDUCTOR_AWS_SECRET = "conductor.mysql.aws.secret";
    private static final String region = "ap-southeast-2";
    private static final int MAX_RETRY = 7;

    private String secretName;
    private String username;
    private String password;
    private ObjectMapper mapper = new ObjectMapper();

    AWSSecret(){
        username = null;
        password = null;
        secretName = getProperty(CONDUCTOR_AWS_SECRET, null);
        if(isNotBlank(secretName)){
            loadSecret(getSecret(secretName));
        }
    }

    private String getSecret(String secretName) {
        logger.info("getSecret:[{}]", secretName);
        final AWSSecretsManager client  = AWSSecretsManagerClientBuilder.standard()
                .withRegion(region)
                .build();

        String secret, decodedBinarySecret;
        GetSecretValueRequest getSecretValueRequest = new GetSecretValueRequest().withSecretId(secretName);
        logger.info("getSecretValueRequest:[{}]", getSecretValueRequest);
        try {
            GetSecretValueResult getSecretValueResult = client.getSecretValue(getSecretValueRequest);
            logger.info("getSecretValueResult:[{}]", getSecretValueResult);
            if (getSecretValueResult.getSecretString() != null) {
                secret = getSecretValueResult.getSecretString();
                logger.trace("secret:[{}]", secret);
                return secret;
            }
            else {
                decodedBinarySecret = new String(getDecoder().decode(getSecretValueResult.getSecretBinary()).array());
                logger.trace("decodedBinarySecret:[{}]", decodedBinarySecret);
                return decodedBinarySecret;
            }
        } catch (DecryptionFailureException | InternalServiceErrorException | InvalidParameterException | InvalidRequestException | ResourceNotFoundException e) {
            logger.error("error getting secret ", e);
            return null;
        }
    }


    private void loadSecret(final String secret){
        if(isBlank(secret)){
            logger.info("Secret payload is empty, skipping...");
            return;
        }
        int retryCount = 0;
        while (retryCount++ <= MAX_RETRY) {
            try {
                JsonNode jsonObject = mapper.readTree(secret);
                username = jsonObject.get("username").asText();
                password = jsonObject.get("password").asText();
                return;
            } catch (Exception e) {
                logger.warn("error loading credential from AWS SECRET:[{}]", secretName);
            }
        }
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }
}
