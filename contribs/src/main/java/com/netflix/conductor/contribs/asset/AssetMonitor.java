package com.netflix.conductor.contribs.asset;

import com.netflix.conductor.common.metadata.events.EventHandler;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.events.JavaEventAction;
import com.netflix.conductor.core.events.ScriptEvaluator;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.HashMap;
import java.util.Map;

@Singleton
public class AssetMonitor implements JavaEventAction {
	private static Logger logger = LoggerFactory.getLogger(AssetMonitor.class);
	private final WorkflowExecutor executor;

	@Inject
	public AssetMonitor(WorkflowExecutor executor) {
		this.executor = executor;
	}

	@Override
	@SuppressWarnings("unchecked")
	public Map<String, Object> handle(EventHandler.Action action, Object payload, String event, String messageId) throws Exception {
		Map<String, Object> parameters = action.getJava_action().getInputParameters();
		Map<String, Object> op = new HashMap<>();

		String workflow = (String)parameters.get("workflow");
		if (StringUtils.isEmpty(workflow)) {
			throw new RuntimeException("No workflow defined");
		}

		// Get the JQ expression
		String assetId = (String)parameters.get("assetId");
		if (StringUtils.isEmpty(assetId)) {
			throw new RuntimeException("No assetId expression defined");
		}

		// Evaluate the expression
		assetId = ScriptEvaluator.evalJq(assetId, payload);

		// Make sure assetId present in the message
		if (StringUtils.isEmpty(assetId)) {
			logger.info("Do noting as no assetId in the message " + messageId);
			return op;
		}

		// Find running WFs
		for (Workflow wf : executor.getRunningWorkflows(workflow)) {

		}

		return op;
	}
}