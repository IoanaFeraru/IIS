package org.datasource.jdbc.views.customersadresses;

import lombok.Value;

@Value
public class CustomerAddressesView {
    private Integer customerId;
    private String address;
    private String city;
    private String countryCode;
    private String postalCode;
    private String email;
    private String socialProvider;
    private String socialPageUrl;
    private String websiteUrl;
}
