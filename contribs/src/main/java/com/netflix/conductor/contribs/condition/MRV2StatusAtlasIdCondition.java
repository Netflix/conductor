package com.netflix.conductor.contribs.condition;

import com.netflix.conductor.core.events.JavaEventCondition;
import com.netflix.conductor.core.events.ScriptEvaluator;

import java.util.Objects;

public class MRV2StatusAtlasIdCondition implements JavaEventCondition {
	@Override
	public boolean evalBool(Object payload) throws Exception {
		Object titleKeys = ScriptEvaluator.evalJqAsObject(".Data.MetadataRepositoryEvent.AtlasId", payload);
		Object statusName = ScriptEvaluator.evalJqAsObject(".Status.Name", payload);
		return Objects.nonNull(titleKeys) && Objects.nonNull(statusName);
	}
}
