package org.datasource.jdbc.views.subscriptiontierpricing;

import org.datasource.jdbc.JDBCDataSourceConnector;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

@Service
public class SubscriptionTierPricingViewBuilder {
    private static Logger logger = Logger.getLogger(SubscriptionTierPricingViewBuilder.class.getName());

    private String SQL_SUB_TIER_PRICING_SELECT =
            "SELECT id, tier_id, monthly_price_usd, valid_from, valid_to, is_active " +
            "FROM SUBSCRIPTION_TIER_PRICING FETCH FIRST 40000 ROWS ONLY";

    private List<SubscriptionTierPricingView> subscriptionTierPricingViewList = new ArrayList<>();

    public List<SubscriptionTierPricingView> getViewList() {
        return this.subscriptionTierPricingViewList;
    }

    public SubscriptionTierPricingViewBuilder build() {
        logger.info(">>> Building SubscriptionTierPricingView ...");
        try (Connection jdbcConnection = jdbcConnector.getConnection()) {
            Statement selectStmt = jdbcConnection.createStatement();
            ResultSet rs = selectStmt.executeQuery(SQL_SUB_TIER_PRICING_SELECT);

            subscriptionTierPricingViewList = new ArrayList<>();
            while (rs.next()) {
                String id = rs.getString("id");
                String tierId = rs.getString("tier_id");
                String monthlyPriceUsd = rs.getString("monthly_price_usd");
                String validFrom = rs.getString("valid_from");
                String validTo = rs.getString("valid_to");
                String isActive = rs.getString("is_active");

                this.subscriptionTierPricingViewList.add(new SubscriptionTierPricingView(id,
                        tierId, monthlyPriceUsd, validFrom, validTo, isActive));
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return this;
    }

    private JDBCDataSourceConnector jdbcConnector;

    public SubscriptionTierPricingViewBuilder(JDBCDataSourceConnector jdbcConnector) {
        this.jdbcConnector = jdbcConnector;
    }
}
