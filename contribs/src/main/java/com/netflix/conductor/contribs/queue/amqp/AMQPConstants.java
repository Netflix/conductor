/**
 * Copyright 2020 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.contribs.queue.amqp;

public class AMQPConstants {

	public static String AMQP_QUEUE_TYPE = "amqp_queue";
	public static String AMQP_EXCHANGE_TYPE = "amqp_exchange";

	public static String PROPERTY_KEY_TEMPLATE = "workflow.event.queues.amqp.%s";

	public static String DEFAULT_CONTENT_TYPE = "application/json";
	public static String DEFAULT_CONTENT_ENCODING = "UTF-8";
	public static String DEFAULT_EXCHANGE_TYPE = "topic";

	public static boolean DEFAULT_DURABLE = true;
	public static boolean DEFAULT_EXCLUSIVE = false;
	public static boolean DEFAULT_AUTO_DELETE = false;

	public static int DEFAULT_DELIVERY_MODE = 2; // Persistent messages
	public static int DEFAULT_BATCH_SIZE = 1;
	public static int DEFAULT_POLL_TIME_MS = 100;
}
