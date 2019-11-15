/**
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License. See LICENSE in the project root for
 * license information.
 */
package com.microsoft.spring.data.gremlin.query.query;

import java.util.HashMap;
import java.util.Map;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.springframework.data.repository.query.Parameter;
import org.springframework.data.repository.query.Parameters;
import org.springframework.data.repository.query.ResultProcessor;
import org.springframework.lang.NonNull;

import com.microsoft.spring.data.gremlin.query.GremlinOperations;
import com.microsoft.spring.data.gremlin.query.paramerter.GremlinParameterAccessor;
import com.microsoft.spring.data.gremlin.query.paramerter.GremlinParametersParameterAccessor;

public class GraphRepositoryGremlinQuery extends AbstractGremlinQuery {
    private final GremlinQueryMethod method;
    private final GremlinOperations operations;
    private final Client gremlinClient;

    public GraphRepositoryGremlinQuery(@NonNull Client gremlinClient, @NonNull GremlinQueryMethod method,
            @NonNull GremlinOperations operations) {
        super(method, operations);
        this.gremlinClient = gremlinClient;
        this.method = method;
        this.operations = operations;
    }

    @Override
    protected GremlinQuery createQuery(GremlinParameterAccessor accessor) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public Object execute(@NonNull Object[] parameters) {
        final GremlinParameterAccessor accessor = new GremlinParametersParameterAccessor(this.method, parameters);
        final ResultProcessor processor = method.getResultProcessor().withDynamicProjection(accessor);
        final Class<?> methodReturnType = processor.getReturnedType().getReturnedType();
        final Map<String, Object> params = this.resolveParams(this.method.getParameters(), parameters);
        final GremlinQuery gremlinQuery = new GremlinQuery(method.getQueryAnnotation(), params);
        return getExecution(accessor).execute(gremlinQuery, methodReturnType);
    }

    @NonNull
    private GremlinQueryExecution getExecution(GremlinParameterAccessor accessor) {
        if (this.method.isPageQuery()) {
            return new GremlinQueryExecution.PageSubmitQueryExecution(gremlinClient, operations, accessor);
        }

        return new GremlinQueryExecution.SubmitQueryExecution(gremlinClient, operations);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    Map<String, Object> resolveParams(Parameters<?, ?> methodParameters, Object[] parameters) {

        final Map<String, Object> resolvedParameters = new HashMap<>();

        for (final Parameter parameter : methodParameters) {
            final int parameterIndex = parameter.getIndex();
            final Object parameterValue = parameters[parameterIndex];

            // Convenience! Client can simply pass Map<String, Object> params, we
            // automatically resolve them to individual parameters.
            // this is to allow the pass through for GremlinClient
            if (parameterValue instanceof Map) {
                resolvedParameters.putAll((Map) parameterValue);
            }
            parameter.getName().ifPresent(parameterName -> resolvedParameters.put(parameterName, parameterValue));
        }

        return resolvedParameters;
    }

    @Override
    @NonNull
    public GremlinQueryMethod getQueryMethod() {
        return this.method;
    }

}
