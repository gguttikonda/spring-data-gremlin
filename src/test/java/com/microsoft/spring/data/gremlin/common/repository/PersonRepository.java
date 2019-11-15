/**
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License. See LICENSE in the project root for
 * license information.
 */
package com.microsoft.spring.data.gremlin.common.repository;

import com.microsoft.spring.data.gremlin.annotation.Query;
import com.microsoft.spring.data.gremlin.common.domain.Person;
import com.microsoft.spring.data.gremlin.repository.GremlinRepository;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

@Repository
public interface PersonRepository extends GremlinRepository<Person, String> {

    @Query("g.V().has('name', name)")
    public Person findPersonByName(@Param("name") String name);

    @Query(value = "g.V().has(label, 'Person').order().by()", countQuery = "g.V().has(label, 'Person').order().by().count()")
    public Page<Person> findPersonsOrderByName(Pageable page);

}
