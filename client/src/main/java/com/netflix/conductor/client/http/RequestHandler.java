/*
 * Copyright 2022 Netflix, Inc.
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
package com.netflix.conductor.client.http;

import java.io.InputStream;
import java.net.URI;

import org.jetbrains.annotations.Nullable;

import com.netflix.conductor.client.exception.RequestHandlerException;

public interface RequestHandler {
    void delete(URI uri) throws RequestHandlerException;

    @Nullable
    InputStream put(URI uri, Object body) throws RequestHandlerException;

    @Nullable
    InputStream post(URI uri, Object body) throws RequestHandlerException;

    @Nullable
    InputStream get(URI uri) throws RequestHandlerException;
}
