package org.datasource.neo4j;

import org.neo4j.ogm.config.Configuration;
import org.neo4j.ogm.session.Session;
import org.neo4j.ogm.session.SessionFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class Neo4JDataSourceConnector {
    @Value("${neo4j.data.source.DB_URL}")
	private String NEO4J_URL;

    // Database credentials
    @Value("${neo4j.data.source.USER}")
    private String USER;
    @Value("${neo4j.data.source.PASS}")
    private String PASS;
	
	public Session getNeo4JSession() {
        Configuration configuration = new Configuration.Builder()
                .uri(NEO4J_URL)
                .credentials(USER,PASS)
                .build();

        SessionFactory sessionFactory = new SessionFactory(
                configuration,
                "org.datasource.neo4j.views.locations");

        return sessionFactory.openSession();
	}
	
}
