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
package com.netflix.conductor.sdk.workflow.utils;

public class InputOutputGetter {

    public enum Field {
        input,
        output
    }

    public static final class Map {
        private String parent;

        public Map(String parent) {
            this.parent = parent;
        }

        public Map getMap(String key) {
            return new Map(parent + "." + key);
        }

        public String get(String key) {
            return parent + "." + key + "}";
        }
    }

    private String name;

    private Field field;

    public InputOutputGetter(String name, Field field) {
        this.name = name;
        this.field = field;
    }

    public String get(String key) {
        return "${" + name + "." + field + "." + key + "}";
    }

    public String getParent() {
        return "${" + name + "." + field + "}";
    }

    public Map getMap(String key) {
        return new Map("${" + name + "." + field + "." + key);
    }

    public static void main(String[] args) {
        InputOutputGetter input = new InputOutputGetter("task2", Field.output);
        System.out.println(input.get("code"));
        System.out.println(input.getMap("users").get("id"));
        System.out.println(input.getMap("users").getMap("address").get("city"));
        System.out.println(input.getMap("users").getMap("address").getMap("zip").get("code"));
    }
}
