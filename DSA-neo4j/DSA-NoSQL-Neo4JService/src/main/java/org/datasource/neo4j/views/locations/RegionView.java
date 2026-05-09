package org.datasource.neo4j.views.locations;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.neo4j.ogm.annotation.Id;
import org.neo4j.ogm.annotation.NodeEntity;
import org.neo4j.ogm.annotation.Relationship;

import java.util.ArrayList;
import java.util.List;

@NodeEntity( label = "Region")
@Data
@AllArgsConstructor
@NoArgsConstructor(force = true)
public class RegionView {
    @Id
    private Long idRegion;
    private String regionName;
    private String regionLoc;

    @Relationship(type = "PART_OF")
    private List<DepartamentView> departaments = new ArrayList<>();
}
