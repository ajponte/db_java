package me.upalate.db.mongo;

import me.upalate.db.DBClient;

import com.mongodb.MongoClientURI;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import org.bson.Document;

import java.util.List;

import static com.mongodb.client.model.Filters.all;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Filters.lte;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.gte;

public class MongoDBClient implements DBClient {

    /** The actual Mongo instance we are working with. */
    private MongoClient mongoClient;

    /** The current database we are performing operations on. */
    private MongoDatabase database;

    /**
     * A new Mongo client connection to a single host and port, using
     * the specified database defined in DBNAME.
     * @param host The host we are connecting to.
     * @param port The port we are connecting to.
     * @param dbName The name of the database we wish to work with.
     */
    public MongoDBClient(String host, long port, String dbName) {
        String uri = "mongodb://" + host + port;
        mongoClient = new MongoClient(new MongoClientURI(uri));
        database = mongoClient.getDatabase(dbName);
    }

    /**
     * Switches the current DB we are working with to a new one.
     *
     * @param dbName The new DB we are switching to.
     * @throws MongoDBException If the db name is null or the empty string.
     */
    public void changeDb(String dbName)
        throws MongoDBException {
        if (dbName == null || dbName.isEmpty()) {
            throw new MongoDBException("DB Name must be non-null");
        }
        database = mongoClient.getDatabase(dbName);
    }

    /**
     * Returns the collection specified by the collection name.
     *
     * @param collectionName The name of the collection we wish to get.
     * @return A {@link MongoCollection} of {@link Document}s.
     * @throws MongoDBException if the collection name is null or the empty String.
     */
    public MongoCollection<Document> getCollection(String collectionName)
        throws MongoDBException {
        if (collectionName == null || collectionName.isEmpty()) {
            throw new MongoDBException("collection name must be non-null");
        }

        return database.getCollection(collectionName);
    }

    /**
     * Iserts the List of Documents into the collection identified by its collection name.
     *
     * @param documents The List of Documents we wish to insert.
     * @param collectionName The name of the collection we are inserting documents into.
     * @throws MongoDBException If the list of documents is empty or null, or the
     *                          collection name is invalid.
     */
    public void insertDocuments(List<Document> documents, String collectionName)
        throws MongoDBException {
        if (documents == null || documents.isEmpty()) {
            throw new MongoDBException("There are no documents to insert");
        }

        getCollection(collectionName).insertMany(documents);
    }

    /**
     * Inserts a single document into the collection.
     *
     * @param document The document we are inserting.
     * @param collectionName The collection we are inserting the document into.
     * @throws MongoDBException If the document or collectionName are null.
     */
    public void insertDocument(Document document, String collectionName)
        throws MongoDBException {
        checkDocument(document);
        if (collectionName == null || collectionName.isEmpty()) {
            throw new MongoDBException("Collection name must be non-null");
        }

        getCollection(collectionName).insertOne(document);
    }

    public String find(MongoOperationType operationType,
                       String key,
                       String value,
                       String collectionName)
        throws MongoDBException {

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
    public MongoCursor<Document> getCursorFromOperation(MongoOperationType operationType,
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

