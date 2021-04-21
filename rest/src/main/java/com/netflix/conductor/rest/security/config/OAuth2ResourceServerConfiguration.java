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

package com.netflix.conductor.rest.security.config;
import java.util.Arrays;

import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.security.oauth2.resource.ResourceServerProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.oauth2.config.annotation.web.configuration.EnableResourceServer;
import org.springframework.security.oauth2.config.annotation.web.configuration.ResourceServerConfigurerAdapter;
import org.springframework.security.oauth2.provider.token.ResourceServerTokenServices;

import com.google.gson.Gson;
import com.netflix.conductor.rest.constants.ConductorRestConstants;
import com.netflix.conductor.rest.security.pojo.ResourceRoleMappingDTO;
import com.netflix.conductor.rest.service.OAuth2UserInfoTokenServices;

@Configuration
@EnableResourceServer
@ConditionalOnProperty(name = "security.oauth2.enabled", havingValue = "true", matchIfMissing = false)
@EnableGlobalMethodSecurity(prePostEnabled = true)
public class OAuth2ResourceServerConfiguration extends ResourceServerConfigurerAdapter
{
    private Logger logger = LoggerFactory.getLogger(OAuth2ResourceServerConfiguration.class);

    @Value("${security.oauth2.resource.mapping:[]}")
    private String resourceMapping;

    @Override
    public void configure(final HttpSecurity http) throws Exception {

        if(null!=this.resourceMapping)
        {
            Gson gson = new Gson();

            ResourceRoleMappingDTO[] resRoleMappingDTOs = gson.fromJson(this.resourceMapping, ResourceRoleMappingDTO[].class);

            if(null!=resRoleMappingDTOs && resRoleMappingDTOs.length > 0)
            {
            	 String finalLog = ConductorRestConstants.STRING_INITIALIZR;

                 finalLog = finalLog + "\nResource Mapping to be done for : "+ resRoleMappingDTOs.length+" path mappings\n";

                 int pathCounter = 1;

                 for(ResourceRoleMappingDTO aResourceRoleMapping : resRoleMappingDTOs)
                 {
                     logger.warn("\n\t Starting to Map: \t"+ aResourceRoleMapping.getEndpoint());

                     finalLog = finalLog + "\n"+pathCounter+")\t Starting to Map: \t"+ aResourceRoleMapping.getEndpoint();

                     String allowedRoles = "";

                     for(String aRole: aResourceRoleMapping.getRoles())
                     {
                         if(ConductorRestConstants.STRING_INITIALIZR.equalsIgnoreCase(allowedRoles))
                         {
                             allowedRoles = aRole;
                         }
                         else
                         {
                             allowedRoles = allowedRoles + ", " + aRole;
                         }
                     }

                     for(String aHTTPMethod: aResourceRoleMapping.getHttp_methods()) {

                         switch(aHTTPMethod) {
                             case ConductorRestConstants.GET:
                                 http.authorizeRequests()
                                         .antMatchers(HttpMethod.GET,
                                                 aResourceRoleMapping.getEndpoint()) .hasAnyAuthority(allowedRoles);

                                 finalLog = finalLog + "\n\n############################## OAUTH2 API Security Mapping - START ####################################"
                                         + "\n\t Mapped URL: "+aResourceRoleMapping.getEndpoint()
                                         + "\n\t Mapped HTTP Method: GET"
                                         + "\n\t Mapped Roles: " + Arrays.asList(aResourceRoleMapping.getRoles()).toString()
                                         + "\n############################## OAUTH2 API Security Mapping - END ######################################\n\n";
                                 break;
                             case ConductorRestConstants.POST:
                                 http.authorizeRequests()
                                         .antMatchers(HttpMethod.POST,
                                                 aResourceRoleMapping.getEndpoint()) .hasAnyAuthority(allowedRoles);
                                 finalLog = finalLog + "\n\n############################## OAUTH2 API Security Mapping - START ####################################"
                                         + "\n\t Mapped URL: "+aResourceRoleMapping.getEndpoint()
                                         + "\n\t Mapped HTTP Method: POST"
                                         + "\n\t Mapped Roles: " + Arrays.asList(aResourceRoleMapping.getRoles()).toString()
                                         + "\n############################## OAUTH2 API Security Mapping - END ######################################\n\n";
                                 break;
                             case ConductorRestConstants.PUT:
                                 http.authorizeRequests()
                                         .antMatchers(HttpMethod.PUT,
                                                 aResourceRoleMapping.getEndpoint()) .hasAnyAuthority(allowedRoles);
                                 finalLog = finalLog + "\n\n############################## OAUTH2 API Security Mapping - START ####################################"
                                         + "\n\t Mapped URL: "+aResourceRoleMapping.getEndpoint()
                                         + "\n\t Mapped HTTP Method: PUT"
                                         + "\n\t Mapped Roles: " + Arrays.asList(aResourceRoleMapping.getRoles()).toString()
                                         + "\n############################## OAUTH2 API Security Mapping - END ######################################\n\n";
                                 break;
                             case ConductorRestConstants.PATCH:
                                 http.authorizeRequests()
                                         .antMatchers(HttpMethod.PATCH,
                                                 aResourceRoleMapping.getEndpoint()) .hasAnyAuthority(allowedRoles);
                                 finalLog = finalLog + "\n\n############################## OAUTH2 API Security Mapping - START ####################################"
                                         + "\n\t Mapped URL: "+aResourceRoleMapping.getEndpoint()
                                         + "\n\t Mapped HTTP Method: PATCH"
                                         + "\n\t Mapped Roles: " + Arrays.asList(aResourceRoleMapping.getRoles()).toString()
                                         + "\n############################## OAUTH2 API Security Mapping - END ######################################\n\n";
                                 break;
                             case ConductorRestConstants.DELETE:
                                 http.authorizeRequests()
                                         .antMatchers(HttpMethod.DELETE,
                                                 aResourceRoleMapping.getEndpoint()) .hasAnyAuthority(allowedRoles);
                                 finalLog = finalLog + "\n\n############################## OAUTH2 API Security Mapping - START ####################################"
                                         + "\n\t Mapped URL: "+aResourceRoleMapping.getEndpoint()
                                         + "\n\t Mapped HTTP Method: DELETE"
                                         + "\n\t Mapped Roles: " + Arrays.asList(aResourceRoleMapping.getRoles()).toString()
                                         + "\n############################## OAUTH2 API Security Mapping - END ######################################\n\n";
                                 break;
                             default:
                                 logger.error("No matching HTTP Method case found for - "+ aHTTPMethod);
                                 break;
                         }
                     }

                     pathCounter++;
                 }

                 logger.warn(finalLog);
            }
            else
            {
                logger.error("NO RESOURCE SECURITY MAPPING DONE");
            }
        }
        else
        {
            logger.error("NO RESOURCE SECURITY MAPPING DONE");
        }

        http.authorizeRequests()
                .antMatchers(HttpMethod.GET, ConductorRestConstants.GENERIC_ROOT_URL).permitAll()
                .anyRequest().authenticated()
                .and().cors().disable().httpBasic().disable()
                .exceptionHandling()
                .authenticationEntryPoint(
                        (request, response, authException) -> response.sendError(HttpServletResponse.SC_UNAUTHORIZED))
                .accessDeniedHandler(
                        (request, response, authException) -> response.sendError(HttpServletResponse.SC_UNAUTHORIZED));
    }
    
    @Autowired
    ResourceServerProperties sso;

    @Bean
    public ResourceServerTokenServices resourceServerTokenServices(){
        return new OAuth2UserInfoTokenServices(sso.getUserInfoUri());
    }
}