package com.netflix.conductor.common.constraints;

import com.netflix.conductor.common.metadata.RetryLogic;
import com.netflix.conductor.common.metadata.tasks.TaskDef;

import javax.validation.Constraint;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import javax.validation.Payload;
import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;

@Documented
@Constraint(validatedBy = RetryDelayPolicyConstraint.RetryDelayPolicyValidator.class)
@Target({TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface RetryDelayPolicyConstraint {
  String message() default "";

  Class<?>[] groups() default {};

  Class<? extends Payload>[] payload() default {};

  class RetryDelayPolicyValidator implements ConstraintValidator<RetryDelayPolicyConstraint, TaskDef> {

    @Override
    public void initialize(RetryDelayPolicyConstraint constraintAnnotation) {
    }

    @Override
    public boolean isValid(TaskDef taskDef, ConstraintValidatorContext context) {
      context.disableDefaultConstraintViolation();

      boolean valid = true;
      if (taskDef.getRetryLogicPolicy() == RetryLogic.RetryLogicPolicy.UNSPECIFIED && taskDef.isRetryDelaySet()) {
        valid = false;
        String message = String.format("TaskDef: %s retryPolicy can't be UNSPECIFIED as retryDelay is set",
            taskDef.getName());
        context.buildConstraintViolationWithTemplate(message).addConstraintViolation();
      }
      return valid;
    }
  }
}
