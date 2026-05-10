package org.datasource.jdbc.views.subscriptioninvoicelines;

import lombok.Value;

@Value
public class SubscriptionInvoiceLinesView {
    private String id;
    private String invoiceId;
    private String productId;
    private String description;
    private int quantity;
    private String unitPriceUsd;
    private String lineTotalUsd;
    private String createdAt;
}
