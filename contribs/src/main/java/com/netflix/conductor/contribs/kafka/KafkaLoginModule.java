package com.netflix.conductor.contribs.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;

import org.apache.commons.lang3.StringUtils;

/**
 * Java security module used to provide JAAS username and password for access to a Kafka topic
 * @author rickfish
 *
 */
public class KafkaLoginModule implements LoginModule {

    String KAFKA_EVENTS_JAAS_USERNAME_PROPERTY_NAME = "kafka.events.jaas.username";
    String KAFKA_DEFAULT_JAAS_USERNAME_PROPERTY_NAME = "kafka.default.jaas.username";
    String KAFKA_DEFAULT_JAAS_USERNAME_DEFAULT_VALUE = "";

    String KAFKA_EVENTS_JAAS_PASSWORD_PROPERTY_NAME = "kafka.events.jaas.password";
    String KAFKA_DEFAULT_JAAS_PASSWORD_PROPERTY_NAME = "kafka.default.jaas.password";
    String KAFKA_DEFAULT_JAAS_PASSWORD_DEFAULT_VALUE = "";

	public KafkaLoginModule() {
		super();
	}
	protected Map<String, Object> sharedState;
	protected Map<String, Object> options;
	@SuppressWarnings("unchecked")
	@Override
	public void initialize(Subject subject, CallbackHandler callbackHandler, Map<String, ?> sharedState, Map<String, ?> options) {
		this.sharedState = (Map<String, Object>) sharedState;
		this.options = (Map<String, Object>) options;
	}

	@Override
	public boolean login() throws LoginException {
		String userName = getProperty(getKafkaEventsJaasUsername(), null);
		String password = getProperty(getKafkaEventsJaasPassword(), null);		
		List<String> messages = new ArrayList<String>();
		
		if (StringUtils.isBlank(userName)) {
			messages.add("The user name is required.");
		}
		
		if (StringUtils.isBlank(password)) {
			messages.add("The password is required.");
		}
		
		if (messages.size() > 0) {
			throw new LoginException(String.join(" ", messages));
		}
		
		sharedState.put("javax.security.auth.login.name", userName);
		sharedState.put("javax.security.auth.login.password", password.toCharArray());
		
		return true;
	}

	@Override
	public boolean logout() throws LoginException {
		return true;
	}
	
	@Override
	public boolean abort() throws LoginException {
		return true;
	}

	@Override
	public boolean commit() throws LoginException {
		return true;
	}


    /**
     * Get the JAAS username for the 'events' topic
     * @return
     */
    String getKafkaDefaultJaasUsername() {
        return getProperty(KAFKA_EVENTS_JAAS_USERNAME_PROPERTY_NAME,
       		KAFKA_DEFAULT_JAAS_USERNAME_DEFAULT_VALUE);
    }

    /**
     * Get the JAAS username for the 'events' topic
     * @return
     */
    String getKafkaEventsJaasUsername() {
        return getProperty(KAFKA_EVENTS_JAAS_USERNAME_PROPERTY_NAME,
            getKafkaDefaultJaasUsername());
    }

    /**
     * There are several uses for Kafka topics. If all the topics are in the same Kafka cluster, this property can be used.
     * Otherwise, the JAAS password for each specific use should be used.
     */
    String getKafkaDefaultJaasPassword() {
        return getProperty(KAFKA_DEFAULT_JAAS_PASSWORD_PROPERTY_NAME,
            KAFKA_DEFAULT_JAAS_PASSWORD_DEFAULT_VALUE);
    }

    /**
     * Get the JAAS password for the 'events' topic
     * @return
     */
    String getKafkaEventsJaasPassword() {
        return getProperty(KAFKA_EVENTS_JAAS_PASSWORD_PROPERTY_NAME,
            getKafkaDefaultJaasPassword());
    }

    public String getProperty(String key, String defaultValue) {

		String val = null;
		try{
			val = System.getenv(key.replace('.','_'));
			if (val == null || val.isEmpty()) {
				val = Optional.ofNullable(System.getProperty(key)).orElse(defaultValue);
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		return val;
	}
}
