package com.netflix.conductor.contribs.queue.amqp;

public enum AMQPConfigurations {

	PROPERTY_CONTENT_TYPE("contentType"), PROPERTY_CONTENT_ENCODING("contentEncoding"), PROPERTY_IS_DURABLE("durable"),
	PROPERTY_IS_EXCLUSIVE("exclusive"), PROPERTY_AUTO_DELETE("autoDelete"), PROPERTY_DELIVERY_MODE("deliveryMode"),
	PROPERTY_EXCHANGE_TYPE("exchangeType"), PROPERTY_MAX_PRIORITY("maxPriority"), PROPERTY_BATCH_SIZE("batchSize"),
	PROPERTY_POLL_TIME_IN_MS("pollTimeInMs"), PROPERTY_HOSTS("hosts"), PROPERTY_USERNAME("username"),
	PROPERTY_PASSWORD("password"), PROPERTY_VIRTUAL_HOST("virtualHost"), PROPERTY_PORT("port"),
	PROPERTY_CONNECTION_TIMEOUT("connectionTimeout"), PROPERTY_USE_NIO("useNio"),

	// queue exchaneg setings
	PARAM_EXCHANGE_TYPE("exchangeType"), PARAM_ROUTING_KEY("routingKey"), PARAM_DELIVERY_MODE("deliveryMode"),
	PARAM_DURABLE("durable"), PARAM_EXCLUSIVE("exclusive"), PARAM_AUTO_DELETE("autoDelete"),
	PARAM_MAX_PRIORITY("maxPriority");

	String propertyName;

	AMQPConfigurations(String propertyName) {
		this.propertyName = propertyName;
	}

	@Override
	public String toString() {
		return propertyName;
	}
}
