package org.datasource;

import org.datasource.neo4j.views.boughtwith.BoughtWithView;
import org.datasource.neo4j.views.boughtwith.BoughtWithViewBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.logging.Logger;

@RestController
@RequestMapping("/neo4j")
public class RESTViewServiceNeo4J {
    private static Logger logger = Logger.getLogger(RESTViewServiceNeo4J.class.getName());

    @RequestMapping(value = "/ping", method = RequestMethod.GET,
            produces = {MediaType.TEXT_PLAIN_VALUE})
    @ResponseBody
    public String ping() {
        logger.info(">>>> DSA-NoSQL-Neo4J:: RESTViewService is Up!");
        return "Ping response from DSA-NoSQL-Neo4J!";
    }

    @RequestMapping(value = "/bought_with", method = RequestMethod.GET,
            produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    @ResponseBody
    public List<BoughtWithView> getBoughtWithView() throws Exception {
        return boughtWithViewBuilder.build().getViewList();
    }

    @Autowired private BoughtWithViewBuilder boughtWithViewBuilder;
}
