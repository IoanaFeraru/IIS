package org.datasource.jdbc.views.subscriptioninvoices;

import lombok.Value;

@Value
public class SubscriptionInvoicesView {
    private String id;
    private String userId;
    private String invoiceType;
    private String status;
    private String subtotalUsd;
    private String taxUsd;
    private String discountUsd;
    private String totalUsd;
    private String subscriptionId;
    private String billingPeriodStart;
    private String billingPeriodEnd;
    private String paidAt;
    private String dueAt;
    private String createdAt;
}
