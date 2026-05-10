package org.datasource.jdbc.views.orderitems;

import org.datasource.jdbc.JDBCDataSourceConnector;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

@Service
public class OrderItemsViewBuilder {
    private static Logger logger = Logger.getLogger(OrderItemsViewBuilder.class.getName());

    private String SQL_ORDER_ITEMS_SELECT =
            "SELECT id, order_id, product_id, quantity, unit_price_usd, line_total_usd, " +
            "fulfilment_status, created_at FROM order_items limit 40000";

    private List<OrderItemsView> orderItemsViewList = new ArrayList<>();

    public List<OrderItemsView> getViewList() {
        return this.orderItemsViewList;
    }

    public OrderItemsViewBuilder build() {
        logger.info(">>> Building OrderItemsView ...");
        try (Connection jdbcConnection = jdbcConnector.getConnection()) {
            Statement selectStmt = jdbcConnection.createStatement();
            ResultSet rs = selectStmt.executeQuery(SQL_ORDER_ITEMS_SELECT);

            orderItemsViewList = new ArrayList<>();
            while (rs.next()) {
                String id = rs.getString("id");
                String orderId = rs.getString("order_id");
                String productId = rs.getString("product_id");
                int quantity = rs.getInt("quantity");
                String unitPriceUsd = rs.getString("unit_price_usd");
                String lineTotalUsd = rs.getString("line_total_usd");
                String fulfilmentStatus = rs.getString("fulfilment_status");
                String createdAt = rs.getString("created_at");

                this.orderItemsViewList.add(new OrderItemsView(id, orderId, productId,
                        quantity, unitPriceUsd, lineTotalUsd, fulfilmentStatus, createdAt));
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return this;
    }

    private JDBCDataSourceConnector jdbcConnector;

    public OrderItemsViewBuilder(JDBCDataSourceConnector jdbcConnector) {
        this.jdbcConnector = jdbcConnector;
    }
}
