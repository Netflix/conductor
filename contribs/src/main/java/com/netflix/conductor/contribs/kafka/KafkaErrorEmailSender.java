package com.netflix.conductor.contribs.kafka;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.bval.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.queue.MessageEventFailure;
import com.netflix.conductor.core.utils.EmailSender;

/**
 * Sends emails to notify users that their Kafka event failed to execute
 * 
 * @author rickfish
 *
 */
public class KafkaErrorEmailSender extends EmailSender {
	private static final Logger logger = LoggerFactory.getLogger(KafkaErrorEmailSender.class);

	/**
	 * Email html styles
	 */
	private static final String STYLE_BODY = "font-family:Arial,Helvetica,sans-serif;font-size:11px;";
	private static final String STYLE_REASON_FOR_EMAIL = "color:red;margin-top:10px;margin-bottom:10px;";
	private static final String STYLE_CONTEXT_DESCRIPTION = "";
	private static final String STYLE_CONTEXT_VALUE = "font-weight:bold;";
	private static final String STYLE_ERROR_HEADING = "text-decoration:underline;margin-top:5px;margin-bottom:2px;font-size:9px;font-weight:bold;font-family:Arial Black,Gadget,sans-serif;";
	private static final String STYLE_ERROR_NUMBER_HEADING = "margin-bottom:10px;";
	private static final String STYLE_ERROR_NUMBER = "text-decoration:underline;";
	private static final String STYLE_ERROR = "";
	private static final String STYLE_NOTE = "color:black;font-size:11px;margin-top:25px;font-family:Arial,Helvetica,sans-serif;font-style:italic;";
	private static final String STYLE_NOTE_EMPHASIS = "font-weight:bold;";
	private static final String STYLE_NOTE_TEXT = "font-weight:normal;";
	
	/**
	 * The environment where the error happened
	 */
	private String environment;
	/**
	 * How many errors can go in one email
	 */
	private int errorsPerEmail = 1;
	/**
	 * The maximum time interval for which we accumulate errors before the error email gets sent
	 */
	private int minutesUntilErrorEmail = 60;
	/**
	 * Besides the intended recipient for the error email, this is a comma-delimited list of additional email addresses that will receive the email.
	 */
	private String additionalRecipients;
	
	private ObjectMapper objectMapper = new ObjectMapper();
	/**
	 * A map of lists of accumulated errors that will be held until the maximum errors are reached or a maximum time interval has elapsed. 
	 * There is an entry for each topic/recipient combination.
	 */
	private Map<String, AccumulatedErrors> currentErrors;
	
	public KafkaErrorEmailSender(Configuration configuration) {
		super(configuration);
		this.errorsPerEmail = configuration.getKafkaEventsErrorsPerEmail();
		this.minutesUntilErrorEmail = configuration.getKafkaEventsMinutesUntilErrorEmail();
		this.environment = configuration.getEnvironment();
		this.currentErrors = Collections.synchronizedMap(new HashMap<String, AccumulatedErrors>());
		this.additionalRecipients = configuration.getKafkaEventsErrorEmailAddlRecipients();
		/*
		 * In order to enforce the maximum time interval before an email is sent, we schedule a thread to check
		 */
		Executors.newScheduledThreadPool(1).scheduleWithFixedDelay(() -> sendEmails(), 5, 5, TimeUnit.MINUTES);
	}
	
	/**
	 * A new error has occurred, so record it and send an email if appropriate
	 * @param topic the topic on which the event that had the error was consumed
	 * @param errorTopic the topic to which the error will go if one was set up
	 * @param toAddress the email address to which the email will get sent
	 * @param error the error
	 */
	public void newError(String topic, String errorTopic, String toAddress, MessageEventFailure error) {
		if(StringUtils.isNotBlank(toAddress)) {
			String[] recipients = toAddress.split(",");
			for(String recipient : recipients) {
				String key = getAccumulatedErrorsKey(topic, recipient);
				AccumulatedErrors errorsForKey = currentErrors.get(key);
				if(errorsForKey == null) {
					/*
					 * This is the first error in a while so we just send the email and set up the AccumulatedErrors object in case
					 * others happen within the accumulation time interval
					 */
					errorsForKey = new AccumulatedErrors();
					errorsForKey.setErrorTopic(errorTopic);
					this.currentErrors.put(key, errorsForKey);
					sendEmail(key, errorTopic, "", error);
				} else {
					List<MessageEventFailure> errors = errorsForKey.getErrors();
					errors.add(error);
					if(errors.size() >= this.errorsPerEmail) {
						long minutes = (System.currentTimeMillis() - errorsForKey.getIntervalStartTimeMs()) / 60000; 
						sendEmail(key, errorTopic, "There were " + errors.size() + " errors for this topic for this email recipient in the last " + 
							minutes + (minutes == 1 ? " minute" : " minutes") + ". That is the maximum errors for an email.", errors);
						/*
						 * If we sent the email because of the maximum errors was hit, clear the errors and reset the time interval to start now
						 */
						errors.clear();
						errorsForKey.setIntervalStartTimeMs(System.currentTimeMillis());
					}
				}
			}
		}
	}
	
	/**
	 * Go through the map of accumulated errors per topic/recipient and send the email if interval has elapsed
	 */
	private void sendEmails() {
		this.currentErrors.keySet().forEach(key -> {
			AccumulatedErrors errorsForKey = currentErrors.get(key);
			List<MessageEventFailure> errors = errorsForKey.getErrors();
			if(System.currentTimeMillis() - errorsForKey.getIntervalStartTimeMs() > (this.minutesUntilErrorEmail * 60000)) {
				if(errors.isEmpty()) {
					/*
					 * If the time period has elapsed but we haven't accumulated any emails, set it up so the next email will be sent right away
					 */
					this.currentErrors.remove(key);
				} else {
					sendEmail(key, errorsForKey.getErrorTopic(), "There were " + errors.size() + " errors for this topic for this email recipient in the last " + 
						this.minutesUntilErrorEmail + (this.minutesUntilErrorEmail == 1 ? " minute" : " minutes")  + 
						". That is the maximum time period in which we accumulate errors.", errors);
					errors.clear();
					errorsForKey.setIntervalStartTimeMs(System.currentTimeMillis());
				}
			}
		});
	}

	/**
	 * Send an email with one error
	 * @param topicAndRecipient the String that is a concatenation of topic and recipient
	 * @param errorTopic the topic to which the error will go if one was set up
	 * @param reasonForEmail a description of why we are sending the email - this will go in a note at the top of the email body
	 * @param error the error that will be in the email body
	 */
	private void sendEmail(String topicAndRecipient, String errorTopic, String reasonForEmail, MessageEventFailure error) {
		sendEmail(topicAndRecipient, errorTopic, reasonForEmail, Collections.singletonList(error));
	}
	
	/**
	 * Send the email 
	 * 
	 * @param topicAndRecipient the String that is a concatenation of topic and recipient
	 * @param errorTopic the topic to which the error will go if one was set up
	 * @param reasonForEmail a description of why we are sending the email - this will go in a note at the top of the email body
	 * @param errors the errors that will be in the email body
	 */
	private void sendEmail(String topicAndRecipient, String errorTopic, String reasonForEmail, List<MessageEventFailure> errors) {
		String[] components = getTopicAndRecipientFromKey(topicAndRecipient);
		if(components.length != 2) {
			return;
		}
		String topic = components[0];
		String recipient = components[1];
		sendEmail(recipient, this.additionalRecipients, getSubject(topic), getBody(topic, errorTopic, reasonForEmail, errors));
	}
	
	/**
	 * Return the subject of the email
	 * @param topic
	 * @return
	 */
	private String getSubject(String topic) {
		StringBuffer subject = new StringBuffer();
		if(this.environment != null) {
			subject.append("[" + this.environment + "] ");
		}
		subject.append("Error processing events in " + topic + " topic");
		return subject.toString();
	}
	
	/**
	 * Return the body of the email
	 *  
	 * @param topic the topic on which the event that had the error was consumed
	 * @param errorTopic the topic to which the error will go if one was set up
	 * @param reasonForEmail a description of why we are sending the email - this will go in a note at the top of the email body
	 * @param errors the errors that will be in the email body
	 * @return
	 */
	private String getBody(String topic, String errorTopic, String reasonForEmail, List<MessageEventFailure> errors) {
		StringBuffer body = new StringBuffer("<html><body><table><div style='" + STYLE_BODY + "'>");
		if(StringUtils.isNotBlank(reasonForEmail)) {
			body.append("<div style='" + STYLE_REASON_FOR_EMAIL + "'>");
			body.append(reasonForEmail);
			body.append("</div>");
		}
		body.append("<div style='" + STYLE_CONTEXT_DESCRIPTION + "'>Topic where " + (errors.size() == 1 ? "event that had error was" : "events that had errors were") + " consumed: <span style='" + STYLE_CONTEXT_VALUE + "'>" + topic + "</span></div>");
		if(errorTopic != null) {
			body.append("<div style='" + STYLE_CONTEXT_DESCRIPTION + "'>Topic where errors will be published: <span style='" + STYLE_CONTEXT_VALUE + "'>" + errorTopic + "</span></div>");
		} else {
			body.append("<div style='" + STYLE_CONTEXT_VALUE + "'>There is no error topic set up for your events topic so errors will not be published</div>");
		}
		body.append("<div style='" + STYLE_ERROR_HEADING + "'>" + (errors.size() == 1 ? "ERROR" : "ERRORS (" + errors.size() + " of them)") + "</div>");
		if(errors != null && errors.size() > 0) {
			for(int i = 0; i < errors.size(); i++) {
				MessageEventFailure error = errors.get(i);
				String message;
				try {
					message = htmlify(this.objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(error));
				} catch(Exception e) {
					message = e.getMessage();
				}
				body.append(
					errors.size() > 1 ? "<div style='" + STYLE_ERROR_NUMBER_HEADING + "'><div style='" + STYLE_ERROR_NUMBER + "'>Error #" + (i+1) + "</div><div style='" + STYLE_ERROR + "'>" + message + "</div></div>" :
						"<div style='" + STYLE_ERROR + "'>" + message + "</div>");
			}
		}
		body.append("</div><div style='" + STYLE_NOTE + "'><span style='" + STYLE_NOTE_EMPHASIS + "'>Note that</span>" + 
			"<span style='" + STYLE_NOTE_TEXT + "'> the next email for errors for this topic for this email recipient (if another happens) won't be sent for at least " + this.minutesUntilErrorEmail + " minutes</span></div>");
		body.append("</div></body></html>");
		return body.toString();
	}
	
	/**
	 * Take a String that will be formatted appropriately for a console log and make it format appropriately in an html body
	 * @param nonHtmlString
	 * @return
	 */
	private String htmlify(String nonHtmlString) {
		String htmlString = nonHtmlString.replace(" ", "&nbsp;");
		htmlString = htmlString.replace("\r\n", "<br />");
		htmlString = htmlString.replace("\r", "<br />");
		htmlString = htmlString.replace("\n", "<br />");
		return htmlString;
	}
	
	/**
	 * Create a key that is appropriate for a key in the AccumulatedErrors map
	 * @param topic
	 * @param toAddress
	 * @return
	 */
	private String getAccumulatedErrorsKey(String topic, String toAddress) {
		return topic + "^" + toAddress;
	}
	
	/**
	 * Return the components of the key for an AccumulatedErrors map entry
	 * @param topicAndRecipient
	 * @return
	 */
	private String[] getTopicAndRecipientFromKey(String topicAndRecipient) {
		String[] components = new String[0];
		if(topicAndRecipient.contains("^")) {
			components = topicAndRecipient.split("\\^");
			if(components.length == 2) {
				return components;
			} else {
				logger.error("sendMail() was given a key (" + topicAndRecipient + ") that does not contain both the topic and the recipient!!!");
			}
		} else {
			logger.error("sendMail() was given a key (" + topicAndRecipient + ") that does not contain the ^ to separate topic and email recipient!!!");
		}
		return components;
	}
	
	/**
	 * Tracks errors that are accumulated for a topic/recipient until a time interval has elapsed or we have accumulated the maximum errors
	 */
    private class AccumulatedErrors {
		private long intervalStartTimeMs = System.currentTimeMillis();
    	private List<MessageEventFailure> errors = Collections.synchronizedList(new ArrayList<MessageEventFailure>());
    	private String errorTopic;
    	public long getIntervalStartTimeMs() {
			return intervalStartTimeMs;
		}
		public void setIntervalStartTimeMs(long intervalStartTimeMs) {
			this.intervalStartTimeMs = intervalStartTimeMs;
		}
		public List<MessageEventFailure> getErrors() {
			return errors;
		}
		@SuppressWarnings("unused")
		public void setErrors(List<MessageEventFailure> errors) {
			this.errors = errors;
		}
		public String getErrorTopic() {
			return errorTopic;
		}
		public void setErrorTopic(String errorTopic) {
			this.errorTopic = errorTopic;
		}
    }
}
