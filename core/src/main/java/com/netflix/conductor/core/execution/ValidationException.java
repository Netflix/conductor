package com.netflix.conductor.core.execution;

import java.util.List;

public class ValidationException extends ApplicationException {
	private final int violationCount;
	private final List<String> errors;

	public ValidationException(final String message, int violationCount, List<String> errors) {
		super(Code.INVALID_INPUT, message);
		this.violationCount = violationCount;
		this.errors = errors;
	}

	public int getViolationCount() {
		return violationCount;
	}

	public List<String> getErrors() {
		return errors;
	}
}
