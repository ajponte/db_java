package me.upalate.db.mongo;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;

import org.bson.Document;

import java.util.List;

import static com.mongodb.client.model.Filters.all;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Filters.lte;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.gte;

public class MongoDBClient extends AbstractMongoDBClient {

    /**
     * A new Mongo client connection to a single host and port, using
     * the specified database defined in DBNAME.
     * @param host The host we are connecting to.
     * @param port The port we are connecting to.
     * @param dbName The name of the database we wish to work with.
     */
    public MongoDBClient(String host, long port, String dbName) {
        super(host, port, dbName);
    }

   @Override
    public void insertDocuments(List<Document> documents, String collectionName)
        throws MongoDBException {
        if (documents == null || documents.isEmpty()) {
            throw new MongoDBException("There are no documents to insert");
        }

        getCollection(collectionName).insertMany(documents);
    }


   @Override
    public void insertDocument(Document document, String collectionName)
        throws MongoDBException {
        checkDocument(document);
        if (collectionName == null || collectionName.isEmpty()) {
            throw new MongoDBException("Collection name must be non-null");
        }

        getCollection(collectionName).insertOne(document);
    }

   @Override
    public String find(MongoOperationType operationType,
                       String key,
                       String value,
                       String collectionName)
        throws MongoDBException {
       
       // Currently returns toString to make the compiler happy.  It should return
       // an object which represents the data in mongo we are retrieving.
       return getCursorFromOperation(operationType,
                                     key,
                                     value,
                                     getCollection(collectionName)).toString();
    }

    /**
     * Returns a cursor pointing to a collection of Documents which match the query
     * defined by the operation type.
     *
     * @param operationType The type of operation query we are performing.
     * @param key The key of the query.
     * @param value The value of the query.
     * @param collection The collection we are querying
     * @return
     */
    private MongoCursor<Document> getCursorFromOperation(MongoOperationType operationType,
                                                        String key,
                                                        String value,
                                                        MongoCollection<Document> collection) {

        MongoCursor<Document> cursor = collection.find().iterator();
        switch (operationType) {
            case GREATER_THAN:
                cursor = collection.find(gt(key, Integer.parseInt(value))).iterator();
                break;
            case GREATER_THAN_OR_EQUALS:
                cursor = collection.find(gte(key, Integer.parseInt(value))).iterator();
                break;
            case LESS_THAN:
                cursor = collection.find(lt(key, Integer.parseInt(value))).iterator();
                break;
            //TODO: the rest of the operations
        }

        return cursor;
    }

    /**
     * Returns the number of documents in the collection.
     *
     * @return The number of documents in the collection.
     */
    public Long getCollectionSize(String collecionName)
        throws MongoDBException {

        return getCollection(collecionName).count();
    }

    /**
     * Verifies that a Document we are inserting into a collection is valid.
     *
     * @param document The Document we are checking.
     * @throws MongoDBException If the Document is invalid.
     */
    private void checkDocument(Document document)
        throws MongoDBException {

        if (document == null) {
            throw new MongoDBException("Document is null");
        }
    }

    /**
     * Verifies that the query we are utilizing is valid.
     *
     * @param query The query to check.
     * @throws MongoDBException If the query is invalid.
     */
    private void checkQuery(String query)
        throws MongoDBException {

        if (query == null || query.isEmpty()) {
            throw new MongoDBException("Query must be non-null");
        }

        // TODO: Use a regex to parse the query for valid operations and format.
    }
}

