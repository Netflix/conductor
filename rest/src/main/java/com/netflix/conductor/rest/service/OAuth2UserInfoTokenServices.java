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

package com.netflix.conductor.rest.service;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.security.oauth2.resource.AuthoritiesExtractor;
import org.springframework.boot.autoconfigure.security.oauth2.resource.FixedAuthoritiesExtractor;
import org.springframework.boot.autoconfigure.security.oauth2.resource.FixedPrincipalExtractor;
import org.springframework.boot.autoconfigure.security.oauth2.resource.PrincipalExtractor;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.common.DefaultOAuth2AccessToken;
import org.springframework.security.oauth2.common.OAuth2AccessToken;
import org.springframework.security.oauth2.common.exceptions.InvalidTokenException;
import org.springframework.security.oauth2.provider.OAuth2Authentication;
import org.springframework.security.oauth2.provider.OAuth2Request;
import org.springframework.security.oauth2.provider.token.ResourceServerTokenServices;
import org.springframework.util.Assert;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.netflix.conductor.rest.constants.ConductorRestConstants;

@ConditionalOnProperty(name = "conductor.api.security.enabled", value = "true", matchIfMissing = false)
public class OAuth2UserInfoTokenServices implements ResourceServerTokenServices {
    protected final Logger logger = LoggerFactory.getLogger(getClass());
    private final String userInfoEndpointUrl;
    private String tokenType = DefaultOAuth2AccessToken.BEARER_TYPE;
    private AuthoritiesExtractor authoritiesExtractor = new FixedAuthoritiesExtractor();
    private PrincipalExtractor principalExtractor = new FixedPrincipalExtractor();
    
    @Value("${security.oauth2.resource.userInfoUriParserPath:roles}")
    private String userInfoUriParserPath;

    public OAuth2UserInfoTokenServices(String userInfoEndpointUrl) {
        this.userInfoEndpointUrl = userInfoEndpointUrl;
    }

    public void setTokenType(String tokenType) {
        this.tokenType = tokenType;
    }

    public void setAuthoritiesExtractor(AuthoritiesExtractor authoritiesExtractor) {
        Assert.notNull(authoritiesExtractor, "AuthoritiesExtractor must not be null");
        this.authoritiesExtractor = authoritiesExtractor;
    }

    public void setPrincipalExtractor(PrincipalExtractor principalExtractor) {
        Assert.notNull(principalExtractor, "PrincipalExtractor must not be null");
        this.principalExtractor = principalExtractor;
    }

    @Override
    public OAuth2Authentication loadAuthentication(String accessToken)
            throws AuthenticationException, InvalidTokenException {
        Map<String, Object> map = getMap(this.userInfoEndpointUrl, accessToken);
        if (map.containsKey("error")) {
            if (this.logger.isDebugEnabled()) {
                this.logger.debug("userinfo returned error: " + map.get("error"));
            }
            throw new InvalidTokenException(accessToken);
        }
        return extractAuthentication(map);
    }
    
    private List<String> extractRoles(JsonElement jsonElement, String[] pathKeys) {
    	
    	List<String> rolesList = new ArrayList<String>();
    	
    	if(jsonElement.isJsonNull() || pathKeys.length == 0)
    	{
    		this.logger.warn("No Roles Path Defined to extract roles for Authorization");
    	}
    	
    	if(pathKeys.length == 1) {
    		if(jsonElement.isJsonArray()) {
    			for(JsonElement roleElement: jsonElement.getAsJsonArray()) {
    				rolesList.add(roleElement.getAsString());
    			}
    		}
    		else if(jsonElement.isJsonObject()) {
    			
    			if(jsonElement.getAsJsonObject().get(pathKeys[0]).isJsonArray()) {
    				for(JsonElement aSubElement: jsonElement.getAsJsonObject().get(pathKeys[0]).getAsJsonArray()) {
    					rolesList.add(aSubElement.getAsString());
    				}
    			}
    			else
    			{
    				rolesList.addAll(Arrays.asList(jsonElement.getAsJsonObject().get(pathKeys[0]).getAsString().split(ConductorRestConstants.COMMA)));
    			}
    		}
    	}
    	else {
    		if (jsonElement.isJsonArray()) {
        		for(JsonElement aJsonElement: jsonElement.getAsJsonArray()) {
        			rolesList.addAll(extractRoles(aJsonElement, pathKeys));
        		}
        	}
        	else if(jsonElement.isJsonObject()) {
        		rolesList = extractRoles(jsonElement.getAsJsonObject().get(pathKeys[0]) , Arrays.copyOfRange(pathKeys, 1, pathKeys.length));
        	}
    	}
    	
    	return rolesList;
    }

    @SuppressWarnings({ "unchecked", "rawtypes", "deprecation" })
	private OAuth2Authentication extractAuthentication(Map<String, Object> map) {
        Object principal = getPrincipal(map);
        List<GrantedAuthority> authorities = this.authoritiesExtractor.extractAuthorities(map);

        try{
            Gson gson = new Gson();
            
            List<String> rolesList = new ArrayList<String>();
            
            if(this.userInfoUriParserPath != null)
            {
            	String[] pathKeys = this.userInfoUriParserPath.split(ConductorRestConstants.FORWARD_SLASH);
            	
            	if(null!=map.get(pathKeys[0]) && pathKeys.length > 0)
            	{
            		JsonElement rootElement = new JsonParser().parse(gson.toJson(map.get(pathKeys[0])));
            		
            		if(rootElement.isJsonNull()) 
            			this.logger.warn("No Roles found as Access Token root element is null");
            		else
            			rolesList = extractRoles(rootElement, Arrays.copyOfRange(pathKeys, 1, pathKeys.length));
            		
            	}
            	else
            	{
            		JsonElement rootElement = new JsonParser().parse(gson.toJson(map.get(pathKeys[0])));
            		
            		if(rootElement.isJsonNull()) {
            			this.logger.warn("No Roles found as Access Token root element is null");
            		}
            		else if(rootElement.isJsonArray()) {
            			for(JsonElement rolesElement: rootElement.getAsJsonArray()) {
            				rolesList.add(rolesElement.getAsString());
            			}
            		}
            		else if(rootElement.isJsonObject()) {
            			rolesList.addAll(Arrays.asList(rootElement.getAsString().split(ConductorRestConstants.COMMA)));            		}
            	}
            }
            else
            {
            	this.logger.warn("No Roles Path Defined to extract roles for Authorization");
            }
            
            if(null!= rolesList && !rolesList.isEmpty())
            {
            	rolesList.forEach(aRole -> {
            		authorities.add(new SimpleGrantedAuthority(aRole));
            	});
            }
        }
        catch(Exception ex)
        {
            logger.warn("Exception while decoding OAuth Token : \n"+ex.getMessage());
        }

        OAuth2Request request = new OAuth2Request(
                (Map)null, null, (Collection)null, true,
                (Set)null, (Set)null, (String)null, (Set)null, (Map)null);
        UsernamePasswordAuthenticationToken token =
                new UsernamePasswordAuthenticationToken(principal, "N/A", authorities);
        token.setDetails(map);
        return new OAuth2Authentication(request, token);
    }

    protected Object getPrincipal(Map<String, Object> map) {
        Object principal = this.principalExtractor.extractPrincipal(map);
        return (principal == null ? "unknown" : principal);
    }

    public OAuth2AccessToken readAccessToken(String accessToken) {
        throw new UnsupportedOperationException("Not supported: read access token");
    }

    /**
     * Get map is a custom implementation and extracts the OAuth2 Access
     * information for the JWS token returned by ADSF
     * @param path
     * @param accessToken
     * @return
     */
    private Map<String, Object> getMap(String path, String accessToken) {
        this.logger.debug("Getting user info from: " + path);

        try {
            DefaultOAuth2AccessToken oauthToken = new DefaultOAuth2AccessToken(accessToken);
            oauthToken.setTokenType(this.tokenType);

            logger.debug("Oauth Token: " + oauthToken.getValue());

            // the token is made up of 3 parts; header, payload and signature
            // grab the payload of the token that is encoded
            String encodedPayload = oauthToken.getValue().split("\\.")[1];

            logger.debug("Token: Encoded Payload: " + encodedPayload);
            String jsonPayload = new String(Base64.getDecoder().decode(encodedPayload.getBytes()));
            logger.debug("JSON Token Payload: " + jsonPayload);

            ObjectMapper mapper = new ObjectMapper();

            return mapper.readValue(jsonPayload, new TypeReference<Map<String, Object>>(){});

        } catch (Exception ex) {
            this.logger.warn("Could not fetch user details: " + ex.getClass() + ", " + ex.getMessage());
            return Collections.<String, Object>singletonMap("error", "Could not fetch user details");
        }
    }
}