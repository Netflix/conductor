package com.netflix.conductor.common.constraints;

import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;

import javax.validation.Constraint;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import javax.validation.Payload;
import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Map;

import static java.lang.annotation.ElementType.FIELD;

/**
 * This constraint checks for a given json schema definition should be valid.
 */
@Documented
@Constraint(validatedBy = JsonSchemaValidityConstraint.JsonSchemaValidator.class)
@Target({FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface JsonSchemaValidityConstraint {
	String message() default "";

	Class<?>[] groups() default {};

	Class<? extends Payload>[] payload() default {};

	class JsonSchemaValidator implements ConstraintValidator<JsonSchemaValidityConstraint, Map> {

		@Override
		public void initialize(final JsonSchemaValidityConstraint constraintAnnotation) {
		}

		@Override
		public boolean isValid(final Map json, final ConstraintValidatorContext context) {
			try (final InputStream resource = this.getClass().getResourceAsStream("/schema_draft7.json")) {
				final JSONObject jsonSchema = new JSONObject(json);
				final JSONObject jsonSchemaDefinition = new JSONObject(new JSONTokener(resource));
				final Schema schema = SchemaLoader.builder().draftV7Support()
						.schemaJson(jsonSchemaDefinition).build().load().build();
				schema.validate(jsonSchema);
			} catch (final JSONException | IOException e) {
				String message = String.format("Invalid json schema definition: %s", e.getMessage());
				context.buildConstraintViolationWithTemplate(message).addConstraintViolation();
				return false;
			} catch (final ValidationException e) {
                e.getAllMessages().forEach(message ->
                        context.buildConstraintViolationWithTemplate(message).addConstraintViolation());
                return false;
            }
			return true;
		}
	}
}
