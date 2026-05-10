package org.datasource.neo4j.views.boughtwith;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor(force = true)
public class BoughtWithView implements Serializable {
    private String product1Id;
    private String product1Name;
    private String product2Id;
    private String product2Name;
    private long coPurchaseCount;
}
