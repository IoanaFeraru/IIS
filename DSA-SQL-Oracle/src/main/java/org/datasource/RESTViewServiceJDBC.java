package org.datasource;

import org.datasource.jdbc.JDBCDataSourceConnector;
import org.datasource.jdbc.views.subscriptioninvoicelines.SubscriptionInvoiceLinesView;
import org.datasource.jdbc.views.subscriptioninvoicelines.SubscriptionInvoiceLinesViewBuilder;
import org.datasource.jdbc.views.subscriptioninvoices.SubscriptionInvoicesView;
import org.datasource.jdbc.views.subscriptioninvoices.SubscriptionInvoicesViewBuilder;
import org.datasource.jdbc.views.subscriptions.SubscriptionsView;
import org.datasource.jdbc.views.subscriptions.SubscriptionsViewBuilder;
import org.datasource.jdbc.views.subscriptiontierpricing.SubscriptionTierPricingView;
import org.datasource.jdbc.views.subscriptiontierpricing.SubscriptionTierPricingViewBuilder;
import org.datasource.jdbc.views.subscriptiontiers.SubscriptionTiersView;
import org.datasource.jdbc.views.subscriptiontiers.SubscriptionTiersViewBuilder;
import org.datasource.jdbc.views.users.UsersView;
import org.datasource.jdbc.views.users.UsersViewBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.logging.Logger;

@RestController
@RequestMapping("/oracle")
public class RESTViewServiceJDBC {
    private static Logger logger = Logger.getLogger(RESTViewServiceJDBC.class.getName());

    @RequestMapping(value = "/ping", method = RequestMethod.GET,
            produces = {MediaType.TEXT_PLAIN_VALUE})
    @ResponseBody
    public String ping() {
        logger.info(">>>> DSA-SQL-Oracle:: RESTViewService is Up!");
        return "Ping response from DSA-SQL-Oracle!";
    }

    @RequestMapping(value = "/users", method = RequestMethod.GET,
            produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    @ResponseBody
    public List<UsersView> getUsersView() {
        return usersViewBuilder.build().getViewList();
    }

    @RequestMapping(value = "/subscriptions", method = RequestMethod.GET,
            produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    @ResponseBody
    public List<SubscriptionsView> getSubscriptionsView() {
        return subscriptionsViewBuilder.build().getViewList();
    }

    @RequestMapping(value = "/subscription_invoices", method = RequestMethod.GET,
            produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    @ResponseBody
    public List<SubscriptionInvoicesView> getSubscriptionInvoicesView() {
        return subscriptionInvoicesViewBuilder.build().getViewList();
    }

    @RequestMapping(value = "/subscription_invoice_lines", method = RequestMethod.GET,
            produces = {MediaType.APPLICATION_JSON_VALUE})
    @ResponseBody
    public List<SubscriptionInvoiceLinesView> getSubscriptionInvoiceLinesView() {
        return subscriptionInvoiceLinesViewBuilder.build().getViewList();
    }

    @RequestMapping(value = "/subscription_tiers", method = RequestMethod.GET,
            produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    @ResponseBody
    public List<SubscriptionTiersView> getSubscriptionTiersView() {
        return subscriptionTiersViewBuilder.build().getViewList();
    }

    @RequestMapping(value = "/subscription_tier_pricing", method = RequestMethod.GET,
            produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    @ResponseBody
    public List<SubscriptionTierPricingView> getSubscriptionTierPricingView() {
        return subscriptionTierPricingViewBuilder.build().getViewList();
    }

    @Autowired private JDBCDataSourceConnector jdbcConnector;
    @Autowired private UsersViewBuilder usersViewBuilder;
    @Autowired private SubscriptionsViewBuilder subscriptionsViewBuilder;
    @Autowired private SubscriptionInvoicesViewBuilder subscriptionInvoicesViewBuilder;
    @Autowired private SubscriptionInvoiceLinesViewBuilder subscriptionInvoiceLinesViewBuilder;
    @Autowired private SubscriptionTiersViewBuilder subscriptionTiersViewBuilder;
    @Autowired private SubscriptionTierPricingViewBuilder subscriptionTierPricingViewBuilder;
}
