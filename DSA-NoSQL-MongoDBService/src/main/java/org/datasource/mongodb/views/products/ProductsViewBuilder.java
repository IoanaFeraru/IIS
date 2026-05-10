package org.datasource.mongodb.views.products;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.datasource.mongodb.MongoDataSourceConnector;
import org.springframework.stereotype.Service;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

@Service
public class ProductsViewBuilder {

    private static Logger logger = Logger.getLogger(ProductsViewBuilder.class.getName());

    private List<ProductView> productsViewList = new ArrayList<>();

    public List<ProductView> getViewList() {
        return this.productsViewList;
    }

    public ProductsViewBuilder build() throws Exception {
        return this.select().map();
    }

    private ProductsViewBuilder select() throws Exception {
        MongoDatabase db = dataSourceConnector.getMongoDatabase();
        MongoCollection<Document> productsCollection = db.getCollection("products");

        productsViewList = new ArrayList<>();

        productsCollection.find().limit(6000).forEach(doc -> {
            ProductView view = new ProductView();

            view.setSellerId(doc.get("seller_id") != null ? doc.get("seller_id").toString() : null);
            view.setName(doc.get("name") != null ? doc.get("name").toString() : null);
            view.setSlug(doc.get("slug") != null ? doc.get("slug").toString() : null);
            view.setProductType(doc.get("product_type") != null ? doc.get("product_type").toString() : null);
            view.setDescription(doc.get("description") != null ? doc.get("description").toString() : null);
            view.setPriceUsd(doc.get("price_usd") != null ? doc.get("price_usd").toString() : null);
            view.setCurrency(doc.get("currency") != null ? doc.get("currency").toString() : null);
            view.setIsActive(doc.get("is_active") != null ? doc.get("is_active").toString() : null);
            view.setCreatedAt(doc.get("created_at") != null ? doc.get("created_at").toString() : null);
            view.setUpdatedAt(doc.get("updated_at") != null ? doc.get("updated_at").toString() : null);

            productsViewList.add(view);
        });
        logger.info(">>> Loaded " + productsViewList.size() + " products");
        return this;

    }

    private ProductsViewBuilder map() {
        return this;
    }

    private MongoDataSourceConnector dataSourceConnector;

    public ProductsViewBuilder(MongoDataSourceConnector dataSourceConnector) {
        this.dataSourceConnector = dataSourceConnector;
    }
}