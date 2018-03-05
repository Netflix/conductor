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
package com.netflix.conductor.common.metadata.workflow;

/**
 * @author Viren
 *
 */
public class SubWorkflowParams {

	private String name;
	
	private Object version;

	private boolean standbyOnFail;

	private boolean restartOnFail;

	private int restartCount = -1; // -1 means an infinite number of restarts
	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @param name the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * @return the version
	 */
	public Object getVersion() {
		return version;
	}

	/**
	 * @param version the version to set
	 */
	public void setVersion(Object version) {
		this.version = version;
	}

	/**
	 * @return Stand By the parent workflow upon sub-workflow failure
	 */
	public boolean isStandbyOnFail() {
		return standbyOnFail;
	}

	/**
	 * @param standbyOnFail Stand By the parent workflow upon sub-workflow failure
	 */
	public void setStandbyOnFail(boolean standbyOnFail) {
		this.standbyOnFail = standbyOnFail;
	}

	/**
	 * @return Restart the sub-workflow upon failure
	 */
	public boolean isRestartOnFail() {
		return restartOnFail;
	}

	/**
	 * @param restartOnFail Restart the sub-workflow upon failure
	 */
	public void setRestartOnFail(boolean restartOnFail) {
		this.restartOnFail = restartOnFail;
	}

	/**
	 * @return  Number of restarts for the sub-workflow
	 */
	public int getRestartCount() {
		return restartCount;
	}

	/**
	 * @param restartCount  Number of restarts for the sub-workflow
	 */
	public void setRestartCount(int restartCount) {
		this.restartCount = restartCount;
	}
}
