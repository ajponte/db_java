package me.upalate.db.mongo;

import org.bson.Document;

import java.util.List;

/**
 * Performs Async operations on Mongo Collections.
 *
 * TODO: Everything.
 */
public class MongoAsyncClient extends AbstractMongoDBClient {

   public MongoAsyncClient(String host, long port, String dbName) {
      super(host, port, dbName);
   }

   @Override
   public void insertDocument(Document document, String collectionName)
      throws MongoDBException {
      // TODO
   }

   @Override
   public void insertDocuments(List<Document> documents, String collectionName)
      throws MongoDBException {
      // TODO
   }

   @Override
   public String find(MongoOperationType operationType,
                      String key,
                      String value,
                      String collectionName) throws MongoDBException {
      // TODO
      return "{}";
   }
}
