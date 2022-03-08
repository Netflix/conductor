/**
 * Copyright 2017 Netflix, Inc.
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
package com.netflix.conductor.common.run;


public class WorkflowError {

    private Integer id;

    private String lookup;

    private Boolean isRequiredInReporting;

    private String totalCount;


    public WorkflowError() {

    }


    public Integer getId() {
        return id;
    }


    public void setId(Integer id) {
        this.id = id;
    }


    public String getLookup() {
        return lookup;
    }


    public void setLookup(String lookup) {
        this.lookup = lookup;
    }

    public String getTotalCount() {
        return totalCount;
    }


    public void setTotalCount(String totalCount) {
        this.totalCount = totalCount;
    }


    public Boolean getIsRequiredInReporting() {
        return isRequiredInReporting;
    }

    public void setIsRequiredInReporting(Boolean isRequiredInReporting) {
        this.isRequiredInReporting = isRequiredInReporting;
    }


}
