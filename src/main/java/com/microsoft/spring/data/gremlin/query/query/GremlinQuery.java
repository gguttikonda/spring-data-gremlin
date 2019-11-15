/**
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License. See LICENSE in the project root for
 * license information.
 */
package com.microsoft.spring.data.gremlin.query.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

import com.microsoft.spring.data.gremlin.annotation.Query;
import com.microsoft.spring.data.gremlin.common.Constants;
import com.microsoft.spring.data.gremlin.query.criteria.Criteria;
import lombok.Getter;
import lombok.NonNull;

public class GremlinQuery {

    @Getter
    private final Criteria criteria;

    @Getter
    private final Query gremlinQuery;

    @Getter
    private final Map<String, Object> params;

    public GremlinQuery(@NonNull Criteria criteria) {
        this.criteria = criteria;
        this.gremlinQuery = null;
        this.params = null;
    }

    public GremlinQuery(@NonNull Query gremlinQuery, @NonNull Map<String, Object> params) {
        this.criteria = null;
        this.gremlinQuery = gremlinQuery;
        this.params = params;
    }

    /**
     * Add .skip().limit() to the end of the query
     * 
     * @param pageable
     * @return updated query
     */
    public String getPageQuery(Pageable pageable) {
        final String query = gremlinQuery.value();
        if (query.endsWith(";")) {
            return addSkipLimit(updateOrderBy(query.substring(0, query.length() - 1), pageable), pageable);
        }
        return addSkipLimit(updateOrderBy(query, pageable), pageable);
    }

    private String updateOrderBy(String gremlin, Pageable pageable) {
        // no order().by() filter, so ignore it
        if (!gremlin.contains("order().by()")) {
            return gremlin;
        }
        final Sort sort = pageable.getSort();
        final List<String> orders = new ArrayList<>();
        sort.forEach(order -> {
            final String property = order.getProperty();
            if (order.isAscending()) {
                orders.add("by('" + property + "', asc)");
            } else {
                orders.add("by('" + property + "', desc)");
            }
        });

        return gremlin.replace("by()", String.join(".", orders));

    }

    private String addSkipLimit(String query, Pageable pageable) {
        final int skip = pageable.getPageNumber() * pageable.getPageSize();
        final int limit = pageable.getPageSize();
        params.put(Constants.SKIP, skip);
        params.put(Constants.LIMIT, limit);
        return query + Constants.SKIP_LIMIT;
    }
}
