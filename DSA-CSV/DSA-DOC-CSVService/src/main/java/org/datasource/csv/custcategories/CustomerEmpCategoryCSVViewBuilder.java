package org.datasource.csv.custcategories;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.datasource.csv.CSVResourceFileDataSourceConnector;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

@Service
public class CustomerEmpCategoryCSVViewBuilder {

    private List<CustomerCategoryView> viewList = new java.util.ArrayList<>();

    public List<CustomerCategoryView> getViewList() {
        return viewList;
    }

    private CSVResourceFileDataSourceConnector dataSourceConnector;
    private File csvFile;

    public CustomerEmpCategoryCSVViewBuilder(CSVResourceFileDataSourceConnector dataSourceConnector) throws Exception {
        this.dataSourceConnector = dataSourceConnector;
        csvFile = dataSourceConnector.getCSVFile();
    }

    // Builder Workflow
    public CustomerEmpCategoryCSVViewBuilder build() throws Exception{
        Reader in = new FileReader(this.csvFile);
        CSVFormat format = CSVFormat.DEFAULT.withFirstRecordAsHeader().withDelimiter(',');
        Iterable<CSVRecord> records = format.parse(in);
        viewList = new ArrayList<>();
        for (CSVRecord record : records) {
            this.viewList.add(new CustomerCategoryView(
                        record.get("Category_code"),
                        record.get("Category_name"),
                        Double.parseDouble(record.get("Lower_L")),
                        Double.parseDouble(record.get("Upper_L"))
                    )
            );
        }
        //
        return this;
    }
}

/*
this.viewList.add(new CustomerCategoryView(
                        record.get(0),
                        record.get(1),
                        Double.parseDouble(record.get(2)),
                        Double.parseDouble(record.get(3))
                    )
            );
 */