package org.datasource.jdbc.views.orders;

import org.datasource.jdbc.JDBCDataSourceConnector;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

@Service
public class OrdersViewBuilder {
    private static Logger logger = Logger.getLogger(OrdersViewBuilder.class.getName());

    private String SQL_ORDERS_SELECT =
            "SELECT id, user_id, invoice_id, status, shipping_name, shipping_address, " +
            "shipping_city, shipping_country, shipping_postal, created_at, updated_at " +
            "FROM orders limit 40000";

    private List<OrdersView> ordersViewList = new ArrayList<>();

    public List<OrdersView> getViewList() {
        return this.ordersViewList;
    }

    public OrdersViewBuilder build() {
        logger.info(">>> Building OrdersView ...");
        try (Connection jdbcConnection = jdbcConnector.getConnection()) {
            Statement selectStmt = jdbcConnection.createStatement();
            ResultSet rs = selectStmt.executeQuery(SQL_ORDERS_SELECT);

            ordersViewList = new ArrayList<>();
            while (rs.next()) {
                String id = rs.getString("id");
                String userId = rs.getString("user_id");
                String invoiceId = rs.getString("invoice_id");
                String status = rs.getString("status");
                String shippingName = rs.getString("shipping_name");
                String shippingAddress = rs.getString("shipping_address");
                String shippingCity = rs.getString("shipping_city");
                String shippingCountry = rs.getString("shipping_country");
                String shippingPostal = rs.getString("shipping_postal");
                String createdAt = rs.getString("created_at");
                String updatedAt = rs.getString("updated_at");

                this.ordersViewList.add(new OrdersView(id, userId, invoiceId, status,
                        shippingName, shippingAddress, shippingCity, shippingCountry,
                        shippingPostal, createdAt, updatedAt));
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return this;
    }

    private JDBCDataSourceConnector jdbcConnector;

    public OrdersViewBuilder(JDBCDataSourceConnector jdbcConnector) {
        this.jdbcConnector = jdbcConnector;
    }
}
