package org.datasource;

import org.datasource.csv.sellerprofiles.SellerProfileView;
import org.datasource.csv.sellerprofiles.SellerProfilesCSVViewBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.logging.Logger;

@RestController
@RequestMapping("/csv")
public class RESTViewServiceCSV {
    private static Logger logger = Logger.getLogger(RESTViewServiceCSV.class.getName());

    @RequestMapping(value = "/seller_profiles", method = RequestMethod.GET,
            produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    @ResponseBody
    public List<SellerProfileView> getSellerProfilesView() throws Exception {
        if (this.sellerProfilesCSVViewBuilder.getViewList().isEmpty())
            return this.sellerProfilesCSVViewBuilder.build().getViewList();
        else
            return this.sellerProfilesCSVViewBuilder.getViewList();
    }

    @Autowired private SellerProfilesCSVViewBuilder sellerProfilesCSVViewBuilder;
}
