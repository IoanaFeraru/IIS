package org.datasource;

import org.datasource.mongodb.views.products.ProductView;
import org.datasource.mongodb.views.products.ProductsViewBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import java.util.List;
import java.util.logging.Logger;

@RestController
@RequestMapping("/mongo")
public class RESTViewServiceMongoDB {
    private static Logger logger = Logger.getLogger(RESTViewServiceMongoDB.class.getName());

    @RequestMapping(value = "/ping", method = RequestMethod.GET,
            produces = {MediaType.TEXT_PLAIN_VALUE})

    @ResponseBody
    public String ping() {
        logger.info(">>>> DSA-NoSQL-MongoDB:: RESTViewService is Up!");
        return "Ping response from DSA-NoSQL-MongoDB!";
    }

    @RequestMapping(value = "/products", method = RequestMethod.GET,
            produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})

    @ResponseBody
    public List<ProductView> getProductsView() throws Exception {
        return productsViewBuilder.build().getViewList();
    }

    @Autowired private ProductsViewBuilder productsViewBuilder;
}