package com.netflix.conductor.contribs.condition;

import com.netflix.conductor.core.events.JavaEventCondition;
import com.netflix.conductor.core.events.ScriptEvaluator;

import java.util.Objects;

public class TitleKeysActionCondition implements JavaEventCondition {
	@Override
	public boolean evalBool(Object payload) throws Exception {
		Object featureId = ScriptEvaluator.evalJqAsObject(".data.titleKeys.featureId", payload);
		Object seasonId = ScriptEvaluator.evalJqAsObject(".data.titleKeys.seasonId", payload);
		Object episodeId = ScriptEvaluator.evalJqAsObject(".data.titleKeys.episodeId", payload);
		Object seriesId = ScriptEvaluator.evalJqAsObject(".data.titleKeys.seriesId", payload);
		Object franchiseId = ScriptEvaluator.evalJqAsObject(".data.titleKeys.franchiseId", payload);
		Object franchiseVersionId = ScriptEvaluator.evalJqAsObject(".data.titleKeys.franchiseVersionId", payload);
		Object seriesVersionId = ScriptEvaluator.evalJqAsObject(".data.titleKeys.seriesVersionId", payload);
		Object seasonVersionId = ScriptEvaluator.evalJqAsObject(".data.titleKeys.seasonVersionId", payload);
		return (Objects.nonNull(featureId) || (Objects.nonNull(seasonId) && Objects.nonNull(episodeId) && Objects.nonNull(seriesId)) ||
				(Objects.nonNull(franchiseId) && Objects.nonNull(franchiseVersionId)) || (Objects.nonNull(seriesId) && Objects.nonNull(seriesVersionId)) ||
				(Objects.nonNull(seasonId) && Objects.nonNull(seasonVersionId)));
	}
}
