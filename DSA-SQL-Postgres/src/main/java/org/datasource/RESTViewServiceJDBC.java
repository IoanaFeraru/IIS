package org.datasource;

import org.datasource.jdbc.JDBCDataSourceConnector;
import org.datasource.jdbc.views.marketplaceinvoicelines.MarketplaceInvoiceLinesView;
import org.datasource.jdbc.views.marketplaceinvoicelines.MarketplaceInvoiceLinesViewBuilder;
import org.datasource.jdbc.views.marketplaceinvoices.MarketplaceInvoicesView;
import org.datasource.jdbc.views.marketplaceinvoices.MarketplaceInvoicesViewBuilder;
import org.datasource.jdbc.views.orderitems.OrderItemsView;
import org.datasource.jdbc.views.orderitems.OrderItemsViewBuilder;
import org.datasource.jdbc.views.orders.OrdersView;
import org.datasource.jdbc.views.orders.OrdersViewBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.logging.Logger;

@RestController
@RequestMapping("/pg")
public class RESTViewServiceJDBC {
    private static Logger logger = Logger.getLogger(RESTViewServiceJDBC.class.getName());

    @RequestMapping(value = "/ping", method = RequestMethod.GET,
            produces = {MediaType.TEXT_PLAIN_VALUE})
    @ResponseBody
    public String ping() {
        logger.info(">>>> DSA-SQL-PostgreSQL:: RESTViewService is Up!");
        return "Ping response from DSA-SQL-PostgreSQL!";
    }

    @RequestMapping(value = "/orders", method = RequestMethod.GET,
            produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    @ResponseBody
    public List<OrdersView> getOrdersView() {
        return ordersViewBuilder.build().getViewList();
    }

    @RequestMapping(value = "/order_items", method = RequestMethod.GET,
            produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    @ResponseBody
    public List<OrderItemsView> getOrderItemsView() {
        return orderItemsViewBuilder.build().getViewList();
    }

    @RequestMapping(value = "/marketplace_invoices", method = RequestMethod.GET,
            produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    @ResponseBody
    public List<MarketplaceInvoicesView> getMarketplaceInvoicesView() {
        return marketplaceInvoicesViewBuilder.build().getViewList();
    }

    @RequestMapping(value = "/marketplace_invoice_lines", method = RequestMethod.GET,
            produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    @ResponseBody
    public List<MarketplaceInvoiceLinesView> getMarketplaceInvoiceLinesView() {
        return marketplaceInvoiceLinesViewBuilder.build().getViewList();
    }

    @Autowired private JDBCDataSourceConnector jdbcConnector;
    @Autowired private OrdersViewBuilder ordersViewBuilder;
    @Autowired private OrderItemsViewBuilder orderItemsViewBuilder;
    @Autowired private MarketplaceInvoicesViewBuilder marketplaceInvoicesViewBuilder;
    @Autowired private MarketplaceInvoiceLinesViewBuilder marketplaceInvoiceLinesViewBuilder;
}
