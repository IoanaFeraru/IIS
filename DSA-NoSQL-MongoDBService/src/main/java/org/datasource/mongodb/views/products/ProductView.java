package org.datasource.mongodb.views.products;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor(force = true)
public class ProductView implements Serializable {

    private String id;
    private String sellerId;
    private String name;
    private String slug;
    private String productType;
    private String description;
    private String priceUsd;
    private String currency;
    private String isActive;
    private String createdAt;
    private String updatedAt;
}