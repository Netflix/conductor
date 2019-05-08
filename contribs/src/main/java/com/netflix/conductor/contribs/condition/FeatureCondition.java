package com.netflix.conductor.contribs.condition;

import com.netflix.conductor.core.events.JavaEventCondition;
import com.netflix.conductor.core.events.ScriptEvaluator;
import org.apache.commons.lang3.StringUtils;

public class FeatureCondition implements JavaEventCondition {
	@Override
	public boolean evalBool(Object payload) throws Exception {
		String featureId = ScriptEvaluator.evalJq(".data.titleKeys.featureId", payload);
		return StringUtils.isNotEmpty(featureId);
	}
}
