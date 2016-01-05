package me.upalate.db.mongo;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.List;

public abstract class AbstractMongoDBClient {

   private String host;
   private long port;
   private String dbName;
   private String uri;
   private MongoDatabase database;
   private MongoClient mongoClient;

   /**
    * A new Mongo client connection to a single host and port, using
    * the specified database defined in DBNAME.
    * @param host The host we are connecting to.
    * @param port The port we are connecting to.
    * @param dbName The name of the database we wish to work with.
    */
   public AbstractMongoDBClient(String host, long port, String dbName) {
      this.host = host;
      this.port = port;
      this.dbName = dbName;
      uri = "mongodb://" + host + port;
      mongoClient = new MongoClient(new MongoClientURI(uri));
      database = mongoClient.getDatabase(dbName);
   }

   /**
    * Inserts a single {@link Document} into the collection.
    *
    * @param document The document we are inserting.
    * @param collectionName The collection we are inserting the document into.
    * @throws MongoDBException If the document or collectionName are null.
    */
   public abstract void insertDocument(Document document, String collectionName)
      throws MongoDBException;

   /**
    * Iserts the List of Documents into the collection identified by its collection name.
    *
    * @param documents The List of Documents we wish to insert.
    * @param collectionName The name of the collection we are inserting documents into.
    * @throws MongoDBException If the list of documents is empty or null, or the
    *                          collection name is invalid.
    */
   public abstract void insertDocuments(List<Document> documents, String collectionName)
      throws MongoDBException;

   /**
    * Performs a find operation on the database.  This operation is similar to
    * db.collectionName.find(key, value) in the CLI.
    *
    * @param operationType The type of operation we are performing.
    * @param key The key of the query.
    * @param value The value of the query.
    * @param collectionName
    * @return
    */
   public abstract String find(MongoOperationType operationType,
                               String key,
                               String value,
                               String collectionName) throws MongoDBException;

   /**
    * Returns the collection specified by the collection name.
    *
    * @param collectionName The name of the collection we wish to get.
    * @return A {@link MongoCollection} of {@link Document}s.
    * @throws MongoDBException if the collection name is null or the empty String.
    */
   public MongoCollection<Document> getCollection(String collectionName) throws MongoDBException {
      if (collectionName == null || collectionName.isEmpty()) {
         throw new MongoDBException("Collection name must be non-null");
      }

      return database.getCollection(collectionName);
   }

   /**
    * Switches the current DB we are working with to a new one.
    *
    * @param dbName The new DB we are switching to.
    * @throws MongoDBException If the db name is null or the empty string.
    */
   protected void changeDB(String dbName) throws MongoDBException {
      if (dbName == null || dbName.isEmpty()) {
         throw new MongoDBException("DB Name must be non-null");
      }

      this.database = mongoClient.getDatabase(dbName);
   }

   /**
    * Returns the current DB we are performing operations on.
    *
    * @return The current DB this object instance is pointing to.
    */
   protected MongoDatabase getDB() {
      return database;
   }
}
