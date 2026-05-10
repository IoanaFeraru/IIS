package org.datasource.jdbc.views.subscriptions;

import lombok.Value;

@Value
public class SubscriptionsView {
    private String id;
    private String userId;
    private String status;
    private String startedAt;
    private String currentPeriodStart;
    private String currentPeriodEnd;
    private String cancelledAt;
    private String cancelReason;
    private String billingCycle;
    private String createdAt;
    private String updatedAt;
    private String tierId;
}
