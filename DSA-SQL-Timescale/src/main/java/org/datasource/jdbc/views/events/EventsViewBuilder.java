package org.datasource.jdbc.views.events;

import org.datasource.jdbc.JDBCDataSourceConnector;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

@Service
public class EventsViewBuilder {
    private static Logger logger = Logger.getLogger(EventsViewBuilder.class.getName());

    private String SQL_EVENTS_SELECT =
            "SELECT id, user_id, event_type, product_id, session_id, metadata::text, occurred_at " +
            "FROM events LIMIT 50000";

    private List<EventsView> eventsViewList = new ArrayList<>();

    public List<EventsView> getViewList() {
        return this.eventsViewList;
    }

    public EventsViewBuilder build() {
        logger.info(">>> Building EventsView ...");
        try (Connection jdbcConnection = jdbcConnector.getConnection()) {
            Statement selectStmt = jdbcConnection.createStatement();
            ResultSet rs = selectStmt.executeQuery(SQL_EVENTS_SELECT);

            eventsViewList = new ArrayList<>();
            while (rs.next()) {
                String id = rs.getString("id");
                String userId = rs.getString("user_id");
                String eventType = rs.getString("event_type");
                String productId = rs.getString("product_id");
                String sessionId = rs.getString("session_id");
                String metadata = rs.getString("metadata");
                String occurredAt = rs.getString("occurred_at");

                this.eventsViewList.add(new EventsView(id, userId, eventType,
                        productId, sessionId, metadata, occurredAt));
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return this;
    }

    private JDBCDataSourceConnector jdbcConnector;

    public EventsViewBuilder(JDBCDataSourceConnector jdbcConnector) {
        this.jdbcConnector = jdbcConnector;
    }
}
