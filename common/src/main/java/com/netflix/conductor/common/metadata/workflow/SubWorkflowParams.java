/**
 * Copyright 2016 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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

import com.github.vmg.protogen.annotations.*;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.util.Map;

/**
 * @author Viren
 *
 */
@ProtoMessage
public class SubWorkflowParams {

    @ProtoField(id = 1)
    @NotNull(message = "SubWorkflowParams name cannot be null")
    @NotEmpty(message = "SubWorkflowParams name cannot be empty")
    private String name;

    @ProtoField(id = 2)
    private Integer version;

    @ProtoField(id = 3)
    private Map<String, String> taskToDomain;

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
    public Integer getVersion() {
        return version;
    }

    /**
     * @param version the version to set
     */
    public void setVersion(Integer version) {
        this.version = version;
    }

    /**
     * @return the taskToDomain
     */
    public Map<String, String> getTaskToDomain() {
        return taskToDomain;
    }
    /**
     * @param taskToDomain the taskToDomain to set
     */
    public void setTaskToDomain(Map<String, String> taskToDomain) {
        this.taskToDomain = taskToDomain;
    }
}
