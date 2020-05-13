package com.netflix.conductor.contribs.condition;

import com.netflix.conductor.core.events.JavaEventCondition;
import com.netflix.conductor.core.events.ScriptEvaluator;

import java.util.Objects;

public class MetadataStatusAtlasIdCondition implements JavaEventCondition {
	@Override
	public boolean evalBool(Object payload) throws Exception {
		Object titleKeys = ScriptEvaluator.evalJqAsObject(".data.metadataRepositoryEvent.atlasId", payload);
		Object statusName = ScriptEvaluator.evalJqAsObject(".status.name", payload);
		return Objects.nonNull(titleKeys) && Objects.nonNull(statusName);
	}
}
