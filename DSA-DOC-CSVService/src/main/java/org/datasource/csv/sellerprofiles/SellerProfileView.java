package org.datasource.csv.sellerprofiles;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor(force = true)
public class SellerProfileView implements Serializable {
    private String userId;
    private String displayName;
    private String legalName;
    private String taxId;
    private String payoutEmail;
    private String countryCode;
    private String isVerified;
    private String bio;
    private String createdAt;
    private String updatedAt;
}
