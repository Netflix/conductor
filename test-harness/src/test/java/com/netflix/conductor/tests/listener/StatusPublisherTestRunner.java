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
package com.netflix.conductor.tests.listener;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import com.netflix.conductor.contribs.DynoQueueStatusPublisherModule;
import com.netflix.conductor.tests.utils.TestModule;
import org.junit.runners.BlockJUnit4ClassRunner;

import static com.netflix.conductor.core.config.Configuration.AVAILABILITY_ZONE_PROPERTY_NAME;
import static com.netflix.conductor.core.config.Configuration.REGION_PROPERTY_NAME;
import static com.netflix.conductor.core.config.Configuration.WORKFLOW_NAMESPACE_PREFIX_PROPERTY_NAME;

public class StatusPublisherTestRunner extends BlockJUnit4ClassRunner {

	private Injector dependenciesInjector;

	static {
		System.setProperty(REGION_PROPERTY_NAME, "us-east-1");
		System.setProperty(AVAILABILITY_ZONE_PROPERTY_NAME, "us-east-1c");
	}


	public StatusPublisherTestRunner(Class<?> klass) throws Exception {
		super(klass);
		System.setProperty(WORKFLOW_NAMESPACE_PREFIX_PROPERTY_NAME, "conductor" + System.getProperty("user.name"));
		dependenciesInjector = Guice.createInjector(Modules.override(new TestModule()).with(new DynoQueueStatusPublisherModule()));
	}

	@Override
	protected Object createTest() throws Exception {
		Object test = super.createTest();
		dependenciesInjector.injectMembers(test);
		return test;
	}
	
	
}
