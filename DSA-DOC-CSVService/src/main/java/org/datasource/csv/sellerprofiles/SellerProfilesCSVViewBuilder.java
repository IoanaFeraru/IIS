package org.datasource.csv.sellerprofiles;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.datasource.csv.CSVResourceFileDataSourceConnector;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

@Service
public class SellerProfilesCSVViewBuilder {
    private static Logger logger = Logger.getLogger(SellerProfilesCSVViewBuilder.class.getName());

    private List<SellerProfileView> viewList = new ArrayList<>();

    public List<SellerProfileView> getViewList() {
        return viewList;
    }

    private CSVResourceFileDataSourceConnector dataSourceConnector;
    private File csvFile;

    public SellerProfilesCSVViewBuilder(CSVResourceFileDataSourceConnector dataSourceConnector) throws Exception {
        this.dataSourceConnector = dataSourceConnector;
        csvFile = dataSourceConnector.getCSVFile();
    }

    public SellerProfilesCSVViewBuilder build() throws Exception {
        logger.info(">>> Building SellerProfilesView from " + csvFile.getAbsolutePath());
        Reader in = new FileReader(this.csvFile);
        CSVFormat format = CSVFormat.DEFAULT.withFirstRecordAsHeader().withDelimiter(',');
        Iterable<CSVRecord> records = format.parse(in);
        viewList = new ArrayList<>();
        for (CSVRecord record : records) {
            this.viewList.add(new SellerProfileView(
                    record.get("user_id"),
                    record.get("display_name"),
                    record.get("legal_name"),
                    record.get("tax_id"),
                    record.get("payout_email"),
                    record.get("country_code"),
                    record.get("is_verified"),
                    record.get("bio"),
                    record.get("created_at"),
                    record.get("updated_at")
            ));
        }
        logger.info(">>> Loaded " + viewList.size() + " seller profiles");
        return this;
    }
}
