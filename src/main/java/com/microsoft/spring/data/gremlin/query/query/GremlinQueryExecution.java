/**
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License. See LICENSE in the project root for
 * license information.
 */
package com.microsoft.spring.data.gremlin.query.query;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.LongSupplier;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.springframework.data.domain.Pageable;
import org.springframework.data.repository.support.PageableExecutionUtils;
import org.springframework.lang.NonNull;
import org.springframework.util.Assert;

import com.microsoft.spring.data.gremlin.annotation.Query;
import com.microsoft.spring.data.gremlin.common.GremlinUtils;
import com.microsoft.spring.data.gremlin.conversion.source.GremlinSource;
import com.microsoft.spring.data.gremlin.query.GremlinOperations;
import com.microsoft.spring.data.gremlin.query.GremlinTemplate;
import com.microsoft.spring.data.gremlin.query.paramerter.GremlinParameterAccessor;

public interface GremlinQueryExecution {
    Object execute(GremlinQuery query, Class<?> type);

    final class FindExecution implements GremlinQueryExecution {

        private final GremlinOperations operations;

        public FindExecution(@NonNull GremlinOperations operations) {
            this.operations = operations;
        }

        @Override
        public Object execute(@NonNull GremlinQuery query, @NonNull Class<?> domainClass) {
            final GremlinSource<?> source = GremlinUtils.toGremlinSource(domainClass);

            return this.operations.find(query, source);
        }
    }
    
    final class SubmitQueryExecution implements GremlinQueryExecution {

        private final GremlinOperations operations;
        private final Client gremlinClient;

        public SubmitQueryExecution(@NonNull Client gremlinClient, @NonNull GremlinOperations operations) {
            this.operations = operations;
            this.gremlinClient = gremlinClient;
        }

        @Override
        public Object execute(@NonNull GremlinQuery query, @NonNull Class<?> domainClass) {
            final GremlinSource<?> source = GremlinUtils.toGremlinSource(domainClass);
            final Map<String, Object> params = query.getParams();
            final Query annotatedQuery = query.getGremlinQuery();
            Assert.notNull(annotatedQuery, "annotatedQuery should not be null");
            final String gremlin = annotatedQuery.value();
            final ResultSet rs = this.gremlinClient.submit(gremlin, params);

            if (ResultSet.class.equals(domainClass)) {
                return rs;
            }

            if (GremlinTemplate.class.equals(this.operations.getClass())) {
                try {
                    final List<Result> gremlinResults = rs.all().get();
                    final List<?> results = ((GremlinTemplate) this.operations).recoverDomainList(source, gremlinResults);

                    if (results != null && results.size() == 1) {
                        // return pojo instead of list
                        return results.get(0);
                    }
                    return results;
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                } catch (ExecutionException e) {
                    throw new IllegalStateException(e);
                }
            }

            throw new UnsupportedOperationException(domainClass + " is not handled by deserializer!");
        }
    }

    final class PageSubmitQueryExecution implements GremlinQueryExecution {
        private final GremlinOperations operations;
        private final Client gremlinClient;
        private final GremlinParameterAccessor accessor;

        public PageSubmitQueryExecution(@NonNull Client gremlinClient, @NonNull GremlinOperations operations,
                @NonNull GremlinParameterAccessor accessor) {
            this.operations = operations;
            this.gremlinClient = gremlinClient;
            this.accessor = accessor;
        }

        @Override
        public Object execute(@NonNull GremlinQuery query, @NonNull Class<?> domainClass) {
            final GremlinSource<?> source = GremlinUtils.toGremlinSource(domainClass);
            final Map<String, Object> params = query.getParams();
            final Query annotatedQuery = query.getGremlinQuery();
            Assert.notNull(annotatedQuery, "annotatedQuery should not be null");
            final String gremlin = query.getPageQuery(accessor.getPageable());
            final ResultSet rs = this.gremlinClient.submit(gremlin, params);

            if (ResultSet.class.equals(domainClass)) {
                return rs;
            }

            if (GremlinTemplate.class.equals(this.operations.getClass())) {
                try {
                    final Long count = executeCountQuery(query, params);
                    final Pageable pageable = accessor.getPageable();
                    final List<?> data = ((GremlinTemplate) this.operations).recoverDomainList(source, rs.all().get());
                    return PageableExecutionUtils.getPage(data, pageable, (LongSupplier) () -> count);
                } catch (InterruptedException | ExecutionException e) {
                    throw new IllegalStateException(e);
                }
            }

            throw new UnsupportedOperationException(domainClass + " is not handled by deserializer!");
        }

        private Long executeCountQuery(@NonNull GremlinQuery query, @NonNull Map<String, Object> params) {
            Assert.notNull(query.getGremlinQuery(), "@Query cannot be null");
            Assert.hasText(query.getGremlinQuery().countQuery(), "countQuery cannot be null");
            try {
                final List<Result> results = this.gremlinClient.submit(query.getGremlinQuery().countQuery(), params)
                        .all().get();
                if (results.isEmpty()) {
                    return 0L;
                }
                return results.get(0).getLong();
            } catch (InterruptedException | ExecutionException e) {
                throw new IllegalStateException(e);
            }

        }
    }
}
