package org.datasource.jdbc.views.orders;

import lombok.Value;

@Value
public class OrdersView {
    private String id;
    private String userId;
    private String invoiceId;
    private String status;
    private String shippingName;
    private String shippingAddress;
    private String shippingCity;
    private String shippingCountry;
    private String shippingPostal;
    private String createdAt;
    private String updatedAt;
}
