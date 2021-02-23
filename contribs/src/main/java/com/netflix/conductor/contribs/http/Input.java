package com.netflix.conductor.contribs.http;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import javax.ws.rs.core.MediaType;
import java.util.HashMap;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Input {

	private String method;    //PUT, POST, GET, DELETE, OPTIONS, HEAD

	private String vipAddress;

	private Map<String, Object> headers = new HashMap<>();

	private String uri;

	private Object body;

	private String contentType;

	private String accept = MediaType.APPLICATION_JSON;

	private String oauthConsumerKey;

	private String oauthConsumerSecret;

	private String serviceDiscoveryQuery;

	private String taskId;

	private String curtimestamp;

	private boolean correlation;

	private boolean authorize;

	private String authorizeParty;

	private Map<String, String> authorizeHeaders;

	private boolean followRedirects = true;

	private boolean traceId;

	/**
	 * @return the method
	 */
	public String getMethod() {
		return method;
	}

	/**
	 * @param method the method to set
	 */
	public void setMethod(String method) {
		this.method = method;
	}

	/**
	 * @return the headers
	 */
	public Map<String, Object> getHeaders() {
		return headers;
	}

	/**
	 * @param headers the headers to set
	 */
	public void setHeaders(Map<String, Object> headers) {
		this.headers = headers;
	}

	/**
	 * @return the body
	 */
	public Object getBody() {
		return body;
	}

	/**
	 * @param body the body to set
	 */
	public void setBody(Object body) {
		this.body = body;
	}

	/**
	 * @return the uri
	 */
	public String getUri() {
		return uri;
	}

	/**
	 * @param uri the uri to set
	 */
	public void setUri(String uri) {
		this.uri = uri;
	}

	/**
	 * @return the vipAddress
	 */
	public String getVipAddress() {
		return vipAddress;
	}

	/**
	 * @param vipAddress the vipAddress to set
	 */
	public void setVipAddress(String vipAddress) {
		this.vipAddress = vipAddress;
	}

	/**
	 * @return the content type
	 */
	public String getContentType() {
		return contentType;
	}

	/**
	 * @param contentType the content type to set
	 */
	public void setContentType(String contentType) {
		this.contentType = contentType;
	}

	/**
	 * @return the accept
	 */
	public String getAccept() {
		return accept;
	}

	/**
	 * @param accept the accept to set
	 */
	public void setAccept(String accept) {
		this.accept = accept;
	}

	/**
	 * @return the OAuth consumer Key
	 */
	public String getOauthConsumerKey() {
		return oauthConsumerKey;
	}

	/**
	 * @param oauthConsumerKey the OAuth consumer key to set
	 */
	public void setOauthConsumerKey(String oauthConsumerKey) {
		this.oauthConsumerKey = oauthConsumerKey;
	}

	/**
	 * @return the OAuth consumer secret
	 */
	public String getOauthConsumerSecret() {
		return oauthConsumerSecret;
	}

	/**
	 * @param oauthConsumerSecret the OAuth consumer secret to set
	 */
	public void setOauthConsumerSecret(String oauthConsumerSecret) {
		this.oauthConsumerSecret = oauthConsumerSecret;
	}

	public void setServiceDiscoveryQuery(String query) {
		this.serviceDiscoveryQuery = query;
	}

	public String getServiceDiscoveryQuery() {
		return serviceDiscoveryQuery;
	}

	/**
	 * @return the task id
	 */
	public String getTaskId() {
		return taskId;
	}

	/**
	 * @param taskId the task id to set
	 */
	public void setTaskId(String taskId) {
		this.taskId = taskId;
	}

	/**
	 * @return the curtimestamp flag
	 */
	public String getCurtimestamp() {
		return curtimestamp;
	}

	/**
	 * @param curtimestamp the curtimestamp to set
	 */
	public void setCurtimestamp(String curtimestamp) {
		this.curtimestamp = curtimestamp;
	}

	/**
	 * @return Deluxe Owf Context flag
	 */
	public boolean isCorrelation() {
		return correlation;
	}

	/**
	 * @param correlation whether Deluxe Owf Context required to be set
	 */
	public void setCorrelation(boolean correlation) {
		this.correlation = correlation;
	}

	/**
	 * @return Authorization require flag
	 */
	public boolean isAuthorize() {
		return authorize;
	}

	/**
	 * @param authorize Authorization require flag
	 */
	public void setAuthorize(boolean authorize) {
		this.authorize = authorize;
	}

	/**
	 * @return Follow 30x redirects
	 */
	public boolean isFollowRedirects() {
		return followRedirects;
	}

	/**
	 * @param followRedirects Follow 30x redirects
	 */
	public void setFollowRedirects(boolean followRedirects) {
		this.followRedirects = followRedirects;
	}

	/**
	 * @return Whether set TraceId or not
	 */
	public boolean isTraceId() {
		return traceId;
	}

	/**
	 * @param traceId Whether set TraceId or not
	 */
	public void setTraceId(boolean traceId) {
		this.traceId = traceId;
	}

	/**
	 * @return External authorize party
	 */
	public String getAuthorizeParty() {
		return authorizeParty;
	}

	/**
	 * @param authorizeParty External authorize party
	 */
	public void setAuthorizeParty(String authorizeParty) {
		this.authorizeParty = authorizeParty;
	}

	/**
	 * @return Additional authorize fields
	 */
	public Map<String, String> getAuthorizeHeaders() {
		return authorizeHeaders;
	}

	/**
	 * @param authorizeHeaders Additional authorize fields
	 */
	public void setAuthorizeHeaders(Map<String, String> authorizeHeaders) {
		this.authorizeHeaders = authorizeHeaders;
	}
}
