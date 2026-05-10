package org.datasource.jdbc.views.orderitems;

import lombok.Value;

@Value
public class OrderItemsView {
    private String id;
    private String orderId;
    private String productId;
    private int quantity;
    private String unitPriceUsd;
    private String lineTotalUsd;
    private String fulfilmentStatus;
    private String createdAt;
}
