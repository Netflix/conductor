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
package com.netflix.conductor.core.config;

import com.google.inject.AbstractModule;

import java.util.List;
import java.util.Map;

/**
 * Commonly used configurations within Conductor.
 *
 * @author Viren
 */
public interface Configuration {

    /**
     * Time Frequency in seconds, at which the workflow sweeper should run to evaluate running workflows.
     *
     * @return time frequency in seconds
     */
    int getSweepFrequency();

    /**
     * Indicate if we should perform a periodic sweep or not.
     *
     * @return true if sweep is disabled.
     */
    boolean disableSweep();


    /**
     * Indicate if we should run async SYSTEM tasks like HTTP in background.
     *
     * @return true if we should not run background SYSTEM tasks
     */
    boolean disableAsyncWorkers();

    /**
     * Unique identifier for this Conductor server. Useful during logging.
     * <b/>
     * Can be host name, IP address or any other meaningful identifier.
     *
     * @return Unique ID of the server.
     */
    String getServerId();

    /**
     * Current environment name. Most commonly used values are:
     * <ul>
     * <li>test</li>
     * <li>prod</li>
     * </ul>
     *
     * @return current environment name
     */
    String getEnvironment();

    /**
     * Current identifier for the app stack. Commonly used values are:
     * <ul>
     * <li>devint</li>
     * <li>testing</li>
     * <li>staging</li>
     * <li>prod</li>
     * </ul>
     *
     * @return name of the stack
     */
    String getStack();

    /**
     * Unique name for this this application. Useful for logging.
     * TODO:: What's the difference between #getAppId and #getServerId??
     *
     * @return this application id
     */
    String getAppId();

    /**
     * When deploying in GDHA (Globally/Geographically Distributed/Dispersed High Availability) configuration,
     * a region can denote data center region.
     * <b/>
     * When deploying AWS, the value could be something like us-east-1, us-west-2 etc.
     *
     * @return Data center region
     */
    String getRegion();

    /**
     * Rack or Zone this app belongs too.
     * <b/>
     * When deploying in AWS, the value could be same as <em>getRegion()</em> method.
     *
     * @return Availability zone / rack
     */
    String getAvailabilityZone();

    /**
     * Get configured property value as integer.
     *
     * @param name         Name of the property
     * @param defaultValue Default value when not specified
     * @return User defined integer property.
     */
    int getIntProperty(String name, int defaultValue);

    /**
     * Get configured property value as string.
     *
     * @param name         Name of the property
     * @param defaultValue Default value when not specified
     * @return User defined string property.
     */
    String getProperty(String name, String defaultValue);


    /**
     * Get all the configured properties.
     * <b/>
     * Returned properties are copies. Making any change to them, doesn't impact
     * original property values.
     *
     * @return Returns all the configurations in a map.
     */
    Map<String, Object> getAll();

    /**
     * Use this to inject additional modules that should be loaded as part of the Conductor server initialization.
     * If you are creating custom tasks, then extend (com.netflix.conductor.core.execution.tasks.WorkflowSystemTask) and  then initialize them as part of the custom modules.
     * See <em>contribs</em> module.
     *
     * @return Provides a list of additional modules to configure.
     */
    default List<AbstractModule> getAdditionalModules() {
        return null;
    }

}
