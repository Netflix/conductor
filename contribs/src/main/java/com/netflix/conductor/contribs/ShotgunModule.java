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
package com.netflix.conductor.contribs;

import com.google.inject.AbstractModule;
import com.netflix.conductor.core.events.shotgun.ShotgunEventQueueProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Oleksiy Lysak
 *
 */
public class ShotgunModule extends AbstractModule {
    private static Logger logger = LoggerFactory.getLogger(ShotgunModule.class);

	@Override
	protected void configure() {
		bind(ShotgunEventQueueProvider.class).asEagerSingleton();
		logger.info("Shotgun Module configured ...");
	}

}
