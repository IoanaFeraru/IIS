package org.datasource.jdbc.views.subscriptioninvoicelines;

import org.datasource.jdbc.JDBCDataSourceConnector;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

@Service
public class SubscriptionInvoiceLinesViewBuilder {
    private static Logger logger = Logger.getLogger(SubscriptionInvoiceLinesViewBuilder.class.getName());

    private String SQL_SUB_INVOICE_LINES_SELECT =
            "SELECT id, invoice_id, product_id, description, quantity, unit_price_usd, " +
            "line_total_usd, created_at FROM SUBSCRIPTION_INVOICE_LINES FETCH FIRST 40000 ROWS ONLY";

    private List<SubscriptionInvoiceLinesView> subscriptionInvoiceLinesViewList = new ArrayList<>();

    public List<SubscriptionInvoiceLinesView> getViewList() {
        return this.subscriptionInvoiceLinesViewList;
    }

    public SubscriptionInvoiceLinesViewBuilder build() {
        logger.info(">>> Building SubscriptionInvoiceLinesView ...");
        try (Connection jdbcConnection = jdbcConnector.getConnection()) {
            Statement selectStmt = jdbcConnection.createStatement();
            ResultSet rs = selectStmt.executeQuery(SQL_SUB_INVOICE_LINES_SELECT);

            subscriptionInvoiceLinesViewList = new ArrayList<>();
            while (rs.next()) {
                String id = rs.getString("id");
                String invoiceId = rs.getString("invoice_id");
                String productId = rs.getString("product_id");
                String description = rs.getString("description");
                int quantity = rs.getInt("quantity");
                String unitPriceUsd = rs.getString("unit_price_usd");
                String lineTotalUsd = rs.getString("line_total_usd");
                String createdAt = rs.getString("created_at");

                this.subscriptionInvoiceLinesViewList.add(new SubscriptionInvoiceLinesView(id,
                        invoiceId, productId, description, quantity, unitPriceUsd,
                        lineTotalUsd, createdAt));
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return this;
    }

    private JDBCDataSourceConnector jdbcConnector;

    public SubscriptionInvoiceLinesViewBuilder(JDBCDataSourceConnector jdbcConnector) {
        this.jdbcConnector = jdbcConnector;
    }
}
