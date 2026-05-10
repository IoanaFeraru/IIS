package org.datasource.jdbc.views.subscriptiontiers;

import org.datasource.jdbc.JDBCDataSourceConnector;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

@Service
public class SubscriptionTiersViewBuilder {
    private static Logger logger = Logger.getLogger(SubscriptionTiersViewBuilder.class.getName());

    private String SQL_SUB_TIERS_SELECT =
            "SELECT id, name, description FROM SUBSCRIPTION_TIERS FETCH FIRST 40000 ROWS ONLY";

    private List<SubscriptionTiersView> subscriptionTiersViewList = new ArrayList<>();

    public List<SubscriptionTiersView> getViewList() {
        return this.subscriptionTiersViewList;
    }

    public SubscriptionTiersViewBuilder build() {
        logger.info(">>> Building SubscriptionTiersView ...");
        try (Connection jdbcConnection = jdbcConnector.getConnection()) {
            Statement selectStmt = jdbcConnection.createStatement();
            ResultSet rs = selectStmt.executeQuery(SQL_SUB_TIERS_SELECT);

            subscriptionTiersViewList = new ArrayList<>();
            while (rs.next()) {
                String id = rs.getString("id");
                String name = rs.getString("name");
                String description = rs.getString("description");

                this.subscriptionTiersViewList.add(new SubscriptionTiersView(id, name, description));
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return this;
    }

    private JDBCDataSourceConnector jdbcConnector;

    public SubscriptionTiersViewBuilder(JDBCDataSourceConnector jdbcConnector) {
        this.jdbcConnector = jdbcConnector;
    }
}
