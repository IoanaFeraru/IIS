package org.datasource.jdbc.views.users;

import lombok.Value;

@Value
public class UsersView {
    private String id;
    private String email;
    private String fullName;
    private String countryCode;
    private String city;
    private String createdAt;
    private String lastLoginAt;
    private String isActive;
}
