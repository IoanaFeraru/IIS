package org.datasource.neo4j.views.boughtwith;

import org.datasource.neo4j.Neo4JDataSourceConnector;
import org.neo4j.ogm.model.Result;
import org.neo4j.ogm.session.Session;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class BoughtWithViewBuilder {
    private static final String BOUGHT_WITH_CYPHER =
            "MATCH (p1:Product)-[r:BOUGHT_WITH]->(p2:Product) " +
            "RETURN p1.id AS product1Id, p1.name AS product1Name, " +
            "p2.id AS product2Id, p2.name AS product2Name, " +
            "r.co_purchase_count AS coPurchaseCount " +
            "ORDER BY r.co_purchase_count DESC";

    private List<BoughtWithView> boughtWithViewList = new ArrayList<>();

    public List<BoughtWithView> getViewList() {
        return this.boughtWithViewList;
    }

    public BoughtWithViewBuilder build() throws Exception {
        return this.select().map();
    }

    private BoughtWithViewBuilder select() throws Exception {
        Session session = dataSourceConnector.getNeo4JSession();
        try {
            Result result = session.query(BOUGHT_WITH_CYPHER, Collections.emptyMap());

            boughtWithViewList = new ArrayList<>();
            for (Map<String, Object> row : result) {
                BoughtWithView view = new BoughtWithView();
                view.setProduct1Id((String) row.get("product1Id"));
                view.setProduct1Name((String) row.get("product1Name"));
                view.setProduct2Id((String) row.get("product2Id"));
                view.setProduct2Name((String) row.get("product2Name"));
                Object coPurchase = row.get("coPurchaseCount");
                view.setCoPurchaseCount(coPurchase != null ? ((Number) coPurchase).longValue() : 0L);
                boughtWithViewList.add(view);
            }
            session.clear();
        } catch (Exception e) {
            session.clear();
            throw new RuntimeException(e);
        }
        return this;
    }

    private BoughtWithViewBuilder map() {
        return this;
    }

    private Neo4JDataSourceConnector dataSourceConnector;

    public BoughtWithViewBuilder(Neo4JDataSourceConnector dataSourceConnector) {
        this.dataSourceConnector = dataSourceConnector;
    }
}
