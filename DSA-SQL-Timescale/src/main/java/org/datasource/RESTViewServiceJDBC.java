package org.datasource;

import org.datasource.jdbc.JDBCDataSourceConnector;
import org.datasource.jdbc.views.events.EventsView;
import org.datasource.jdbc.views.events.EventsViewBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.logging.Logger;

@RestController
@RequestMapping("/ts")
public class RESTViewServiceJDBC {
    private static Logger logger = Logger.getLogger(RESTViewServiceJDBC.class.getName());

    @RequestMapping(value = "/ping", method = RequestMethod.GET,
            produces = {MediaType.TEXT_PLAIN_VALUE})
    @ResponseBody
    public String ping() {
        logger.info(">>>> DSA-SQL-TimescaleDB:: RESTViewService is Up!");
        return "Ping response from DSA-SQL-TimescaleDB!";
    }

    @RequestMapping(value = "/events", method = RequestMethod.GET,
            produces = {MediaType.APPLICATION_JSON_VALUE})
    @ResponseBody
    public List<EventsView> getEventsView() {
        return eventsViewBuilder.build().getViewList();
    }

    @Autowired private JDBCDataSourceConnector jdbcConnector;
    @Autowired private EventsViewBuilder eventsViewBuilder;
}
