package com.netflix.conductor.core.events;

public interface JavaEventCondition {
	boolean evalBool(Object payload) throws Exception;
}
