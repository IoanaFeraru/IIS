package org.datasource.jdbc.views.subscriptiontierpricing;

import lombok.Value;

@Value
public class SubscriptionTierPricingView {
    private String id;
    private String tierId;
    private String monthlyPriceUsd;
    private String validFrom;
    private String validTo;
    private String isActive;
}
