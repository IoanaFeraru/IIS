package org.datasource.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

@Service
public class MongoDataSourceConnector {
    @Value("${mongodb.data.source.DB_URL}")
	private String mongodbUri;

    @Value("${mongodb.data.source.database}")
    private String mongodbName;

	public MongoDatabase getMongoDatabase() {
        CodecProvider pojoCodecProvider = PojoCodecProvider.builder().automatic(true).build();
        CodecRegistry pojoCodecRegistry = fromRegistries(getDefaultCodecRegistry(), fromProviders(pojoCodecProvider));
        //
        MongoClient mongoClient = MongoClients.create(mongodbUri);
        MongoDatabase db = mongoClient.getDatabase(mongodbName).withCodecRegistry(pojoCodecRegistry);
        //
        return db;
    }
}

/*
* https://www.mongodb.com/docs/drivers/java/sync/v5.2/data-formats/document-data-format-pojo/
 */

    /*
	public MongoDatabase getMongoDatabase() {
		ConnectionString connectionString = new ConnectionString(mongodbUri);
        CodecRegistry pojoCodecRegistry = fromProviders(PojoCodecProvider.builder().automatic(true).build());
        CodecRegistry codecRegistry = fromRegistries(MongoClientSettings.getDefaultCodecRegistry(), pojoCodecRegistry);
        MongoClientSettings clientSettings = MongoClientSettings.builder()
                                                                .applyConnectionString(connectionString)
                                                                .codecRegistry(codecRegistry)
                                                                .build();
        //
        MongoClient mongoClient = MongoClients.create(clientSettings);
        MongoDatabase db = mongoClient.getDatabase(mongodbName);
        //
        return db;
	}

     */
