package org.datasource.jdbc.views.marketplaceinvoices;

import org.datasource.jdbc.JDBCDataSourceConnector;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

@Service
public class MarketplaceInvoicesViewBuilder {
    private static Logger logger = Logger.getLogger(MarketplaceInvoicesViewBuilder.class.getName());

    private String SQL_MKT_INVOICES_SELECT =
            "SELECT id, user_id, invoice_type, status, subtotal_usd, tax_usd, discount_usd, " +
            "total_usd, subscription_id, billing_period_start, billing_period_end, " +
            "paid_at, due_at, created_at FROM marketplace_invoices limit 40000";

    private List<MarketplaceInvoicesView> marketplaceInvoicesViewList = new ArrayList<>();

    public List<MarketplaceInvoicesView> getViewList() {
        return this.marketplaceInvoicesViewList;
    }

    public MarketplaceInvoicesViewBuilder build() {
        logger.info(">>> Building MarketplaceInvoicesView ...");
        try (Connection jdbcConnection = jdbcConnector.getConnection()) {
            Statement selectStmt = jdbcConnection.createStatement();
            ResultSet rs = selectStmt.executeQuery(SQL_MKT_INVOICES_SELECT);

            marketplaceInvoicesViewList = new ArrayList<>();
            while (rs.next()) {
                String id = rs.getString("id");
                String userId = rs.getString("user_id");
                String invoiceType = rs.getString("invoice_type");
                String status = rs.getString("status");
                String subtotalUsd = rs.getString("subtotal_usd");
                String taxUsd = rs.getString("tax_usd");
                String discountUsd = rs.getString("discount_usd");
                String totalUsd = rs.getString("total_usd");
                String subscriptionId = rs.getString("subscription_id");
                String billingPeriodStart = rs.getString("billing_period_start");
                String billingPeriodEnd = rs.getString("billing_period_end");
                String paidAt = rs.getString("paid_at");
                String dueAt = rs.getString("due_at");
                String createdAt = rs.getString("created_at");

                this.marketplaceInvoicesViewList.add(new MarketplaceInvoicesView(id, userId,
                        invoiceType, status, subtotalUsd, taxUsd, discountUsd, totalUsd,
                        subscriptionId, billingPeriodStart, billingPeriodEnd, paidAt, dueAt, createdAt));
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return this;
    }

    private JDBCDataSourceConnector jdbcConnector;

    public MarketplaceInvoicesViewBuilder(JDBCDataSourceConnector jdbcConnector) {
        this.jdbcConnector = jdbcConnector;
    }
}
