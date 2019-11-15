/**
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License. See LICENSE in the project root for
 * license information.
 */
package com.microsoft.spring.data.gremlin.repository;

import java.io.Serializable;

import org.springframework.data.repository.NoRepositoryBean;
import org.springframework.data.repository.PagingAndSortingRepository;

import com.microsoft.spring.data.gremlin.common.GremlinEntityType;

@NoRepositoryBean
public interface GremlinRepository<T, ID extends Serializable> extends PagingAndSortingRepository<T, ID> {

    Iterable<T> findAll(Class<T> domainClass);

    void deleteAll(GremlinEntityType type);

    void deleteAll(Class<T> domainClass);

    long vertexCount();

    long edgeCount();
}
