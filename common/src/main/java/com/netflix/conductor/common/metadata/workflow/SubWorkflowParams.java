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

	private Boolean standbyOnFail;

	private Boolean restartOnFail;

	private Integer restartCount;

	private Long restartDelay;

	private RerunWorkflowParams rerunWorkflow;

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
	public Boolean isStandbyOnFail() {
		return standbyOnFail;
	}

	/**
	 * @param standbyOnFail Stand By the parent workflow upon sub-workflow failure
	 */
	public void setStandbyOnFail(Boolean standbyOnFail) {
		this.standbyOnFail = standbyOnFail;
	}

	/**
	 * @return Restart the sub-workflow upon failure
	 */
	public Boolean isRestartOnFail() {
		return restartOnFail;
	}

	/**
	 * @param restartOnFail Restart the sub-workflow upon failure
	 */
	public void setRestartOnFail(Boolean restartOnFail) {
		this.restartOnFail = restartOnFail;
	}

	/**
	 * @return  Number of restarts for the sub-workflow
	 */
	public Integer getRestartCount() {
		return restartCount;
	}

	/**
	 * @param restartCount  Number of restarts for the sub-workflow
	 */
	public void setRestartCount(Integer restartCount) {
		this.restartCount = restartCount;
	}

	/**
	 * @return Restart delay for the sub-workflow
	 */
	public Long getRestartDelay() {
		return restartDelay;
	}

	/**
	 * @param restartDelay Restart delay for the sub-workflow
	 */
	public void setRestartDelay(Long restartDelay) {
		this.restartDelay = restartDelay;
	}

	/**
	 * @return Rerun workflow details
	 */
	public RerunWorkflowParams getRerunWorkflow() {
		return rerunWorkflow;
	}

	/**
	 * @param rerunWorkflow Rerun workflow details
	 */
	public void setRerunWorkflow(RerunWorkflowParams rerunWorkflow) {
		this.rerunWorkflow = rerunWorkflow;
	}
}
