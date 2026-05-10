package org.datasource.jdbc.views.subscriptions;

import org.datasource.jdbc.JDBCDataSourceConnector;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

@Service
public class SubscriptionsViewBuilder {
    private static Logger logger = Logger.getLogger(SubscriptionsViewBuilder.class.getName());

    private String SQL_SUBSCRIPTIONS_SELECT =
            "SELECT id, user_id, status, started_at, current_period_start, current_period_end, " +
            "cancelled_at, cancel_reason, billing_cycle, created_at, updated_at, tier_id " +
            "FROM SUBSCRIPTIONS FETCH FIRST 40000 ROWS ONLY";

    private List<SubscriptionsView> subscriptionsViewList = new ArrayList<>();

    public List<SubscriptionsView> getViewList() {
        return this.subscriptionsViewList;
    }

    public SubscriptionsViewBuilder build() {
        logger.info(">>> Building SubscriptionsView ...");
        try (Connection jdbcConnection = jdbcConnector.getConnection()) {
            Statement selectStmt = jdbcConnection.createStatement();
            ResultSet rs = selectStmt.executeQuery(SQL_SUBSCRIPTIONS_SELECT);

            subscriptionsViewList = new ArrayList<>();
            while (rs.next()) {
                String id = rs.getString("id");
                String userId = rs.getString("user_id");
                String status = rs.getString("status");
                String startedAt = rs.getString("started_at");
                String currentPeriodStart = rs.getString("current_period_start");
                String currentPeriodEnd = rs.getString("current_period_end");
                String cancelledAt = rs.getString("cancelled_at");
                String cancelReason = rs.getString("cancel_reason");
                String billingCycle = rs.getString("billing_cycle");
                String createdAt = rs.getString("created_at");
                String updatedAt = rs.getString("updated_at");
                String tierId = rs.getString("tier_id");

                this.subscriptionsViewList.add(new SubscriptionsView(id, userId, status,
                        startedAt, currentPeriodStart, currentPeriodEnd, cancelledAt,
                        cancelReason, billingCycle, createdAt, updatedAt, tierId));
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return this;
    }

    private JDBCDataSourceConnector jdbcConnector;

    public SubscriptionsViewBuilder(JDBCDataSourceConnector jdbcConnector) {
        this.jdbcConnector = jdbcConnector;
    }
}
