package com.netflix.conductor.contribs.condition;

import com.netflix.conductor.core.events.JavaEventCondition;
import com.netflix.conductor.core.events.ScriptEvaluator;

import java.util.Objects;

public class OriginalTitleKeysActionCondition implements JavaEventCondition {
	@Override
	public boolean evalBool(Object payload) throws Exception {
		Object featureId = ScriptEvaluator.evalJqAsObject(".data.originalTitleKeys.featureId", payload);
		Object seasonId = ScriptEvaluator.evalJqAsObject(".data.originalTitleKeys.seasonId", payload);
		Object episodeId = ScriptEvaluator.evalJqAsObject(".data.originalTitleKeys.episodeId", payload);
		Object seriesId = ScriptEvaluator.evalJqAsObject(".data.originalTitleKeys.seriesId", payload);
		Object franchiseId = ScriptEvaluator.evalJqAsObject(".data.originalTitleKeys.franchiseId", payload);
		Object franchiseVersionId = ScriptEvaluator.evalJqAsObject(".data.originalTitleKeys.franchiseVersionId", payload);
		Object seriesVersionId = ScriptEvaluator.evalJqAsObject(".data.originalTitleKeys.seriesVersionId", payload);
		Object seasonVersionId = ScriptEvaluator.evalJqAsObject(".data.originalTitleKeys.seasonVersionId", payload);
		return (Objects.nonNull(featureId) || (Objects.nonNull(seasonId) && Objects.nonNull(episodeId) && Objects.nonNull(seriesId)) ||
				(Objects.nonNull(franchiseId) && Objects.nonNull(franchiseVersionId)) || (Objects.nonNull(seriesId) && Objects.nonNull(seriesVersionId)) ||
				(Objects.nonNull(seasonId) && Objects.nonNull(seasonVersionId)));
	}
}
