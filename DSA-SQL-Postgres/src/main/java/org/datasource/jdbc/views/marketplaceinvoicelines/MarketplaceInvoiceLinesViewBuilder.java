package org.datasource.jdbc.views.marketplaceinvoicelines;

import org.datasource.jdbc.JDBCDataSourceConnector;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

@Service
public class MarketplaceInvoiceLinesViewBuilder {
    private static Logger logger = Logger.getLogger(MarketplaceInvoiceLinesViewBuilder.class.getName());

    private String SQL_MKT_INVOICE_LINES_SELECT =
            "SELECT id, invoice_id, product_id, description, quantity, unit_price_usd, " +
            "line_total_usd, created_at FROM marketplace_invoice_lines limit 40000";

    private List<MarketplaceInvoiceLinesView> marketplaceInvoiceLinesViewList = new ArrayList<>();

    public List<MarketplaceInvoiceLinesView> getViewList() {
        return this.marketplaceInvoiceLinesViewList;
    }

    public MarketplaceInvoiceLinesViewBuilder build() {
        logger.info(">>> Building MarketplaceInvoiceLinesView ...");
        try (Connection jdbcConnection = jdbcConnector.getConnection()) {
            Statement selectStmt = jdbcConnection.createStatement();
            ResultSet rs = selectStmt.executeQuery(SQL_MKT_INVOICE_LINES_SELECT);

            marketplaceInvoiceLinesViewList = new ArrayList<>();
            while (rs.next()) {
                String id = rs.getString("id");
                String invoiceId = rs.getString("invoice_id");
                String productId = rs.getString("product_id");
                String description = rs.getString("description");
                int quantity = rs.getInt("quantity");
                String unitPriceUsd = rs.getString("unit_price_usd");
                String lineTotalUsd = rs.getString("line_total_usd");
                String createdAt = rs.getString("created_at");

                this.marketplaceInvoiceLinesViewList.add(new MarketplaceInvoiceLinesView(id,
                        invoiceId, productId, description, quantity, unitPriceUsd,
                        lineTotalUsd, createdAt));
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return this;
    }

    private JDBCDataSourceConnector jdbcConnector;

    public MarketplaceInvoiceLinesViewBuilder(JDBCDataSourceConnector jdbcConnector) {
        this.jdbcConnector = jdbcConnector;
    }
}
