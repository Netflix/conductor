/*
 * Copyright 2020 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.contribs.queue.amqp;

/** Holds the messaging constants. */
public final class MessageConstants {
    private MessageConstants() {}

    public static final String TTL = "ttl";
    public static final String PERSIST_MSGS = "persist-msgs";
    public static final String MSG_PRIORITY = "msg-priority";
    public static final String ENCODING_TYPE = "encoding-type";
    public static final String ENCODING_FORMAT = "UTF-8";
    public static final String MEDIA_TYPE = "application/json";
    public static final String QUEUE_MSG_PRIORITY = "x-max-priority";
    public static final String QUEUE_MSG_TTL = "x-message-ttl";

    // err msgs
    public static final String ERR_INVALID_PAYLOAD =
            "The content specified in the request is not valid";
    public static final String ERR_PAYLOAD_SIZE_EXCEEDED = "Payload size cannot exceed %s %s on %s";
    public static final String ERR_TTL_INVALID = "Time-to-live value cannot be a negative number";

    // err msgs
    public static final String ERR_INVALID_CREDENTIALS = "Specified credentilas are invalid";
    public static final String ERR_INVALID_CREDENTIALS_FILE_PATH =
            "Credentials file can not be found";
    public static final String ERR_CONN_INVALID = "The connection name specified is not valid";
    public static final String ERR_CONN_GET_FAILED = "Failed to create a client connection. %s";
    public static final String ERR_CONN_CLOSURE_FAILED =
            "Failed to close the client connection" + " for the connection type [%s]. %s";

    // err & info topic related
    public static final String ERR_TOPIC_NAME_EMPTY = "Topic name cannot be empty";
    public static final String ERR_TOPIC_EXIST_CHECK = "Failed to check the topic availability";
    public static final String ERR_TOPIC_NOT_FOUND = "No topic found with the name [%s]";
    public static final String ERR_TOPIC_CREATE_FAILED = "Failed to create the topic [%s]. %s";
    public static final String INFO_TOPIC_CREATE_RETRY =
            "Retry attempt [%d] - Creating the topic [%s]";
    public static final String WARN_TOPIC_CREATE_RETRIES_EXHAUSTED =
            "The maximum number of retry attempts" + " to create the topic [%s] is reached";
    public static final String INFO_TOPIC_CREATED = "Created the topic [%s]";
    public static final String ERR_TOPIC_DELETE_FAILED = "Failed to delete the topic [%s]. %s";
    public static final String INFO_TOPIC_DELETE_RETRY =
            "Retry attempt [%d] - Deleting the topic [%s]";
    public static final String WARN_TOPIC_DELETE_RETRIES_EXHAUSTED =
            "The maximum number of retry attempts to delete" + " the topic [%s] is reached";
    public static final String INFO_TOPIC_DELETED = "Deleted the topic [%s]";
    public static final String INFO_TOPIC_EXISTS = "Topic [%s] already exists";
    public static final String INFO_TOPIC_EXISTS_RETRY =
            "Retry attempt [%d] - Checking if the topic [%s] " + "is available";
    public static final String WARN_TOPIC_EXISTS_RETRIES_EXHAUSTED =
            "The maximum number of retry attempts to verify"
                    + " the topic [%s] availability is reached";
    public static final String INFO_TOPIC_PUBLISH_MESSAGE_RETRY =
            "Retry attempt [%d] - Publishing a message to the" + " topic [%s] with label [%s]";
    public static final String WARN_TOPIC_PUBLISH_MESSAGE_RETRIES_EXHAUSTED =
            "The maximum number of retry attempts"
                    + " to publish a message for the topic [%s] with label [%s] is reached";

    // err & info subscription related
    public static final String ERR_SUBSCRIPTION_NAME_EMPTY = "Subscription name cannot be empty";
    public static final String ERR_SUBSCRIPTION_CREATE_FAILED =
            "Failed to create the subscription [%s]. %s";
    public static final String INFO_SUBSCRIPTION_CREATED = "Created subscription [%s]";
    public static final String INFO_SUBSCRIPTION_CREATE_RETRY =
            "Retry attempt [%d] - Creating subscription [%s] with" + " topic [%s]";
    public static final String WARN_SUBSCRIPTION_CREATE_RETRIES_EXHAUSTED =
            "The maximum number of retry attempts"
                    + " to create the subscription [%s] for the topic [%s] is reached";
    public static final String ERR_SUBSCRIPTION_DELETE_FAILED =
            "Failed to delete the subscription [%s]. %s";
    public static final String INFO_SUBSCRIPTION_DELETED =
            "Deleted subscription with the name [%s]";
    public static final String INFO_SUBSCRIPTION_DELETE_RETRY =
            "Retry attempt [%d] - Deleting the subscription [%s]" + " with topic [%s]";
    public static final String WARN_SUBSCRIPTION_DELETE_RETRIES_EXHAUSTED =
            "The maximum number of retry attempts to"
                    + " delete"
                    + " the subscription [%s] with the topic [%s] is reached";
    public static final String INFO_SUBSCRIPTION_RETRY =
            "Retry attempt [%d] - Subscribing with consumer tag [%s] and" + " label [%s]";
    public static final String INFO_SUBSCRIPTION_RETRIES_EXHAUSTED =
            "The maximum number of retry attempts to"
                    + "  subscribe [%s] to the topic [%s] is reached";
    public static final String ERR_SUBSCRIPTION_EXIST_CHECK =
            "Failed to check the subscription availability";
    public static final String INFO_SUBSCRIPTION_EXISTS_RETRY =
            "Retry attempt [%d] - Checking if the subscription "
                    + "[%s]"
                    + " with topic [%s] already available";
    public static final String WARN_SUBSCRIPTION_EXISTS_RETRIES_EXHAUSTED =
            "The maximum number of retry attempts "
                    + "to verify the"
                    + " subscription [%s] availability for the topic [%s] is reached";
    public static final String ERR_SUBSCRIPTION_NOT_FOUND = "Subscription [%s] not found";
    public static final String ERR_SUBSCRIPTION_DELIVERYCALLBACK_NOT_FOUND =
            "Callback [%s] not found";
    public static final String INFO_SUBSCRIPTION_EXISTS = "Subscription [%s] already exists";
    public static final String INFO_SUBSCRIPTION_UNSUBRCIBE_RETRY =
            "Retry attempt [%d] - Removing the subscription " + "[%s] with topic [%s]";
    public static final String WARN_SUBSCRIPTION_UNSUBSCRIBE_RETRIES_EXHAUSTED =
            "Subscription [%s] removal " + "retry attempts for the topic [%s] have exhausted";
    // err queue related
    public static final String ERR_QUEUE_NAME_EMPTY = "Queue name field cannot be empty";
    public static final String ERR_QUEUE_CREATE_FAILED = "Failed to create the queue [%s]. %s";
    public static final String INFO_QUEUE_CREATED = "Created the queue [%s]";
    public static final String ERR_QUEUE_DELETE_FAILED = "Failed to the delete queue [%s]. %s";
    public static final String INFO_QUEUE_DELETED = "Deleted the queue [%s]";
    public static final String ERR_QUEUE_EXIST_CHECK = "Failed to check the queue availability";
    public static final String ERR_QUEUE_NOT_FOUND = "Queue [%s] not found";
    public static final String INFO_QUEUE_EXISTS = "Queue [%s] already exists";

    // err message related
    public static final String ERR_MSG_PROCESSING_FAILED =
            "Failed to process the content for the message [%s]";
    public static final String ERR_MSG_DECODING_FAILED =
            "Failed to decode the content for the message with the" + " consumer tag [%s]";
    public static final String ERR_MSG_REJECTED = "Message with ID [%s] is rejected. %s";
    public static final String ERR_MSG_ACK_FAILED =
            "Failed to acknowledge the message with consumer tag [%s]";
    public static final String ERR_MSG_REJECTION_FAILED =
            "Failed to reject the message with consumer tag [%s]";
    public static final String ERR_MSG_PUBLISH_FAILED =
            "Failed to publish a message to the topic with the name" + " [%s]. %s";
    public static final String INFO_MSG_PUBLISH_STARTED =
            "Publishing a message with ID [%s] to the topic [%s]";
    public static final String INFO_MSG_PUBLISH_COMPLETED =
            "Published a message with ID [%s] to the topic [%s]";
    public static final String INFO_MSG_RECEIVED =
            "Received a message with ID [%s] having consumer tag [%s] from" + " the topic [%s]";

    // callback registration successful
    public static final String INFO_TOPIC_REG_SUCCEEDED =
            "Callback handler is registered for the topic"
                    + " [%s] with subscription [%s] including consumer tag [%s] and the label [%s]";
    public static final String INFO_TOPIC_BIND_SUCCEEDED =
            "Callback handler bind is successful for the topic"
                    + " [%s] with queue [%s] including consumer tag [%s] and the label [%s]";
    public static final String INFO_TOPIC_DEREGISTER_SUCCEEDED =
            "Callback handler is de-registered for the topic"
                    + " [%s] with subscription [%s] and the label [%s]";
    public static final String INFO_TOPIC_UNBIND_SUCCEEDED =
            "Callback handler unbind is successful for the topic"
                    + " [%s] with the queue [%s] and the label [%s]";
    public static final String INFO_TOPIC_REG_FAILED =
            "Callback handler registration failed for the topic"
                    + " [%s] with subscription [%s] and label [%s]. %s";
    public static final String INFO_TOPIC_BIND_FAILED =
            "Callback handler binding failed for the topic"
                    + " [%s] with queue [%s] and label [%s]. %s";
    public static final String INFO_TOPIC_DEREGISTER_FAILED =
            "Callback handler de-registration failed for the topic"
                    + " [%s] with subscription [%s] and label [%s]. %s";
    public static final String INFO_TOPIC_UNBIND_FAILED =
            "Callback handler unbound failed for the topic"
                    + " [%s] with queue [%s] and label [%s]. %s";
    public static final String INFO_TOPIC_REG_CANCELLED =
            "Callback handler is cancelled for the topic"
                    + " [%s] with subscription or queue [%s] including consumer tag [%s] and the label [%s]";

    // info channel messages.
    public static final String INFO_CHANNEL_BORROW_SUCCESS =
            "Borrowed the channel object from the channel pool for " + "the connection type [%s]";
    public static final String INFO_CHANNEL_RETURN_SUCCESS =
            "Returned the borrowed channel object to the pool for " + "the connection type [%s]";
    public static final String INFO_CHANNEL_CREATION_SUCCESS =
            "Channels are not available in the pool. Created a"
                    + " channel for the connection type [%s]";
    public static final String INFO_CHANNEL_RESET_SUCCESS =
            "No proper channels available in the pool. Created a "
                    + "channel for the connection type [%s]";
}
