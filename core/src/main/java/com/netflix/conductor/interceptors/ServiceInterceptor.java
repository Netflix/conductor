/*
 * Copyright 2020 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.interceptors;

import com.google.inject.Inject;
import java.lang.reflect.Modifier;
import java.util.Set;
import javax.inject.Provider;
import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;
import javax.validation.Validator;
import javax.validation.executable.ExecutableValidator;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;

/**
 * Intercept method calls annotated with {@link com.netflix.conductor.annotations.Service}
 * and runs hibernate validations on it.
 *
 */
public class ServiceInterceptor implements MethodInterceptor{

    private Provider<Validator> validatorProvider;

    @Inject
    public ServiceInterceptor(Provider<Validator> validator) {
        this.validatorProvider = validator;
    }

    /**
     *
     * @param invocation
     * @return
     * @throws ConstraintViolationException incase of any constraints
     * defined on method parameters are violated.
     */
    @Override
    public Object invoke(MethodInvocation invocation) throws Throwable {

        if (skipMethod(invocation)) {
            return invocation.proceed();
        }

        ExecutableValidator executableValidator = validatorProvider.get().forExecutables();

        Set<ConstraintViolation<Object>> result = executableValidator.validateParameters(
                invocation.getThis(), invocation.getMethod(), invocation.getArguments());

        if (!result.isEmpty()) {
            throw new ConstraintViolationException(result);
        }

        return invocation.proceed();
    }

    private boolean skipMethod(MethodInvocation invocation) {
        // skip non-public methods or methods on Object class.
        return !Modifier.isPublic( invocation.getMethod().getModifiers() ) || invocation.getMethod().getDeclaringClass().equals( Object.class );
    }
}
