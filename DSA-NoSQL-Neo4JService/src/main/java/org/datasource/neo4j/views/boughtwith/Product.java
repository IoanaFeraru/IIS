package org.datasource.neo4j.views.boughtwith;

import org.neo4j.ogm.annotation.Id;
import org.neo4j.ogm.annotation.NodeEntity;

@NodeEntity(label = "Product")
public class Product {
    @Id
    private String id;
    private String name;
}
