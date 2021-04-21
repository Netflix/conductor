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

package com.netflix.conductor.rest.security.pojo;

import java.util.Arrays;

public class ResourceRoleMappingDTO {

	private String endpoint;
	private String[] http_methods;
	private String[] roles;
	
	public ResourceRoleMappingDTO() {
		super();
	}

	public ResourceRoleMappingDTO(String endpoint, String[] http_methods, String[] roles) {
		super();
		this.endpoint = endpoint;
		this.http_methods = http_methods;
		this.roles = roles;
	}

	public String getEndpoint() {
		return endpoint;
	}

	public void setEndpoint(String endpoint) {
		this.endpoint = endpoint;
	}

	public String[] getHttp_methods() {
		return http_methods;
	}

	public void setHttp_methods(String[] http_methods) {
		this.http_methods = http_methods;
	}

	public String[] getRoles() {
		return roles;
	}

	public void setRoles(String[] roles) {
		this.roles = roles;
	}

	@Override
	public String toString() {
		return "ResourceRoleMappingDTO [endpoint=" + endpoint + ", http_methods=" + Arrays.toString(http_methods)
				+ ", roles=" + Arrays.toString(roles) + "]";
	}
}