/**
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 *
 */
package com.netflix.conductor.contribs.queue.sqs;

import com.amazonaws.auth.policy.Action;
import com.amazonaws.auth.policy.Policy;
import com.amazonaws.auth.policy.Principal;
import com.amazonaws.auth.policy.Resource;
import com.amazonaws.auth.policy.Statement;
import com.amazonaws.auth.policy.Statement.Effect;
import com.amazonaws.auth.policy.actions.SQSActions;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.BatchResultErrorEntry;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.DeleteMessageBatchResult;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.ListQueuesRequest;
import com.amazonaws.services.sqs.model.ListQueuesResult;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.SendMessageBatchResult;
import com.amazonaws.services.sqs.model.SetQueueAttributesResult;
import com.google.common.annotations.VisibleForTesting;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import com.netflix.conductor.metrics.Monitors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Scheduler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author Viren
 *
 */
public class SQSObservableQueue implements ObservableQueue {

	private static Logger logger = LoggerFactory.getLogger(SQSObservableQueue.class);

	private static final String QUEUE_TYPE = "sqs";

	private String queueName;

	private int visibilityTimeoutInSeconds;

	private int batchSize;

	private AmazonSQSClient client;

	private int pollTimeInMS;

	private String queueURL;

	private final Scheduler scheduler;

	private SQSObservableQueue(String queueName,
							   AmazonSQSClient client,
							   int visibilityTimeoutInSeconds,
							   int batchSize,
							   int pollTimeInMS,
							   List<String> accountsToAuthorize,
							   Scheduler scheduler) {
		this.queueName = queueName;
		this.client = client;
		this.visibilityTimeoutInSeconds = visibilityTimeoutInSeconds;
		this.batchSize = batchSize;
		this.pollTimeInMS = pollTimeInMS;
		this.queueURL = getOrCreateQueue();
		this.scheduler = scheduler;
		addPolicy(accountsToAuthorize);
	}

	@Override
	public Observable<Message> observe() {
		OnSubscribe<Message> subscriber = getOnSubscribe();
		return Observable.create(subscriber);
	}

	@Override
	public List<String> ack(List<Message> messages) {
		return delete(messages);
	}

	@Override
	public void publish(List<Message> messages) {
		publishMessages(messages);
	}

	@Override
	public long size() {
		GetQueueAttributesResult attributes = client.getQueueAttributes(queueURL, Collections.singletonList("ApproximateNumberOfMessages"));
		String sizeAsStr = attributes.getAttributes().get("ApproximateNumberOfMessages");
		try {
			return Long.parseLong(sizeAsStr);
		} catch(Exception e) {
			return -1;
		}
	}

	@Override
	public void setUnackTimeout(Message message, long unackTimeout) {
		int unackTimeoutInSeconds = (int) (unackTimeout / 1000);
		ChangeMessageVisibilityRequest request = new ChangeMessageVisibilityRequest(queueURL, message.getReceipt(), unackTimeoutInSeconds);
		client.changeMessageVisibility(request);
	}

	@Override
	public String getType() {
		return QUEUE_TYPE;
	}

	@Override
	public String getName() {
		return queueName;
	}

	@Override
	public String getURI() {
		return queueURL;
	}

	public int getPollTimeInMS() {
		return pollTimeInMS;
	}

	public int getBatchSize() {
		return batchSize;
	}

	public int getVisibilityTimeoutInSeconds() {
		return visibilityTimeoutInSeconds;
	}

	public static class Builder {

		private String queueName;

		private int visibilityTimeout = 30;	//seconds

		private int batchSize = 5;

		private int pollTimeInMS = 100;

		private AmazonSQSClient client;

		private List<String> accountsToAuthorize = new LinkedList<>();

		private Scheduler scheduler;

		public Builder withQueueName(String queueName) {
			this.queueName = queueName;
			return this;
		}

		/**
		 *
		 * @param visibilityTimeout Visibility timeout for the message in SECONDS
		 * @return builder instance
		 */
		public Builder withVisibilityTimeout(int visibilityTimeout) {
			this.visibilityTimeout = visibilityTimeout;
			return this;
		}

		public Builder withBatchSize(int batchSize) {
			this.batchSize = batchSize;
			return this;
		}

		public Builder withClient(AmazonSQSClient client) {
			this.client = client;
			return this;
		}

		public Builder withPollTimeInMS(int pollTimeInMS) {
			this.pollTimeInMS = pollTimeInMS;
			return this;
		}

		public Builder withAccountsToAuthorize(List<String> accountsToAuthorize) {
			this.accountsToAuthorize = accountsToAuthorize;
			return this;
		}

		public Builder addAccountToAuthorize(String accountToAuthorize) {
			this.accountsToAuthorize.add(accountToAuthorize);
			return this;
		}

		public Builder withScheduler(Scheduler scheduler) {
			this.scheduler = scheduler;
			return this;
		}

		public SQSObservableQueue build() {
			return new SQSObservableQueue(queueName, client, visibilityTimeout, batchSize, pollTimeInMS, accountsToAuthorize, scheduler);
		}
	}

	//Private methods
	@VisibleForTesting
	String getOrCreateQueue() {
        List<String> queueUrls = listQueues(queueName);
		if (queueUrls == null || queueUrls.isEmpty()) {
            CreateQueueRequest createQueueRequest = new CreateQueueRequest().withQueueName(queueName);
            CreateQueueResult result = client.createQueue(createQueueRequest);
            return result.getQueueUrl();
		} else {
            return queueUrls.get(0);
        }
    }

	private String getQueueARN() {
		GetQueueAttributesResult response = client.getQueueAttributes(queueURL, Collections.singletonList("QueueArn"));
		return response.getAttributes().get("QueueArn");
	}

	private void addPolicy(List<String> accountsToAuthorize) {
		if(accountsToAuthorize == null || accountsToAuthorize.isEmpty()) {
			logger.info("No additional security policies attached for the queue " + queueName);
			return;
		}
		logger.info("Authorizing " + accountsToAuthorize + " to the queue " + queueName);
		Map<String, String> attributes = new HashMap<>();
		attributes.put("Policy", getPolicy(accountsToAuthorize));
		SetQueueAttributesResult result = client.setQueueAttributes(queueURL, attributes);
		logger.info("policy attachment result: " + result);
		logger.info("policy attachment result: status=" + result.getSdkHttpMetadata().getHttpStatusCode());
	}

	private String getPolicy(List<String> accountIds) {
		Policy policy = new Policy("AuthorizedWorkerAccessPolicy");
		Statement stmt = new Statement(Effect.Allow);
		Action action = SQSActions.SendMessage;
		stmt.getActions().add(action);
		stmt.setResources(new LinkedList<>());
		for(String accountId : accountIds) {
			Principal principal = new Principal(accountId);
			stmt.getPrincipals().add(principal);
		}
		stmt.getResources().add(new Resource(getQueueARN()));
		policy.getStatements().add(stmt);
		return policy.toJson();
	}

	private List<String> listQueues(String queueName) {
        ListQueuesRequest listQueuesRequest = new ListQueuesRequest().withQueueNamePrefix(queueName);
        ListQueuesResult resultList = client.listQueues(listQueuesRequest);
        return resultList.getQueueUrls().stream()
				.filter(queueUrl -> queueUrl.contains(queueName))
				.collect(Collectors.toList());
    }

	private void publishMessages(List<Message> messages) {
		logger.debug("Sending {} messages to the SQS queue: {}", messages.size(), queueName);
		SendMessageBatchRequest batch = new SendMessageBatchRequest(queueURL);
		messages.forEach(msg -> {
			SendMessageBatchRequestEntry sendr = new SendMessageBatchRequestEntry(msg.getId(), msg.getPayload());
			batch.getEntries().add(sendr);
		});
		logger.debug("sending {} messages in batch", batch.getEntries().size());
		SendMessageBatchResult result = client.sendMessageBatch(batch);
		logger.debug("send result: {} for SQS queue: {}", result.getFailed().toString(), queueName);
	}

	@VisibleForTesting
	List<Message> receiveMessages() {
		try {
			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest()
					.withQueueUrl(queueURL)
					.withVisibilityTimeout(visibilityTimeoutInSeconds)
					.withMaxNumberOfMessages(batchSize);

			ReceiveMessageResult result = client.receiveMessage(receiveMessageRequest);

			List<Message> messages = result.getMessages().stream()
					.map(msg -> new Message(msg.getMessageId(), msg.getBody(), msg.getReceiptHandle()))
					.collect(Collectors.toList());
			Monitors.recordEventQueueMessagesProcessed(QUEUE_TYPE, this.queueName, messages.size());
			return messages;
		} catch (Exception e) {
			logger.error("Exception while getting messages from SQS", e);
			Monitors.recordObservableQMessageReceivedErrors(QUEUE_TYPE);
		}
		return new ArrayList<>();
	}

	@VisibleForTesting
	OnSubscribe<Message> getOnSubscribe() {
		return subscriber -> {
			Observable<Long> interval = Observable.interval(pollTimeInMS, TimeUnit.MILLISECONDS);
			interval.flatMap((Long x)->{
				List<Message> msgs = receiveMessages();
		        return Observable.from(msgs);
			}).subscribe(subscriber::onNext, subscriber::onError);
		};
	}

	private List<String> delete(List<Message> messages) {
		if (messages == null || messages.isEmpty()) {
            return null;
        }

        DeleteMessageBatchRequest batch = new DeleteMessageBatchRequest().withQueueUrl(queueURL);
    	List<DeleteMessageBatchRequestEntry> entries = batch.getEntries();

        messages.forEach(m -> entries.add(new DeleteMessageBatchRequestEntry().withId(m.getId()).withReceiptHandle(m.getReceipt())));

        DeleteMessageBatchResult result = client.deleteMessageBatch(batch);
        List<String> failures = result.getFailed().stream()
				.map(BatchResultErrorEntry::getId)
				.collect(Collectors.toList());
		logger.debug("Failed to delete messages from queue: {}: {}", queueName, failures);
        return failures;
    }
}
