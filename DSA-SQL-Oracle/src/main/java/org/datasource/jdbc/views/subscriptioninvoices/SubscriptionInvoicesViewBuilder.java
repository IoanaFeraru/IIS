package org.datasource.jdbc.views.subscriptioninvoices;

import org.datasource.jdbc.JDBCDataSourceConnector;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

@Service
public class SubscriptionInvoicesViewBuilder {
    private static Logger logger = Logger.getLogger(SubscriptionInvoicesViewBuilder.class.getName());

    private String SQL_SUB_INVOICES_SELECT =
            "SELECT id, user_id, invoice_type, status, subtotal_usd, tax_usd, discount_usd, " +
            "total_usd, subscription_id, billing_period_start, billing_period_end, " +
            "paid_at, due_at, created_at FROM SUBSCRIPTION_INVOICES FETCH FIRST 40000 ROWS ONLY";

    private List<SubscriptionInvoicesView> subscriptionInvoicesViewList = new ArrayList<>();

    public List<SubscriptionInvoicesView> getViewList() {
        return this.subscriptionInvoicesViewList;
    }

    public SubscriptionInvoicesViewBuilder build() {
        logger.info(">>> Building SubscriptionInvoicesView ...");
        try (Connection jdbcConnection = jdbcConnector.getConnection()) {
            Statement selectStmt = jdbcConnection.createStatement();
            ResultSet rs = selectStmt.executeQuery(SQL_SUB_INVOICES_SELECT);

            subscriptionInvoicesViewList = new ArrayList<>();
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

                this.subscriptionInvoicesViewList.add(new SubscriptionInvoicesView(id, userId,
                        invoiceType, status, subtotalUsd, taxUsd, discountUsd, totalUsd,
                        subscriptionId, billingPeriodStart, billingPeriodEnd, paidAt, dueAt, createdAt));
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return this;
    }

    private JDBCDataSourceConnector jdbcConnector;

    public SubscriptionInvoicesViewBuilder(JDBCDataSourceConnector jdbcConnector) {
        this.jdbcConnector = jdbcConnector;
    }
}
