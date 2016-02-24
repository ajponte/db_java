package me.upalate.db.mongo;

import com.mongodb.Mongo;

/**
 * An Exception to be thrown in the event of mongo-related
 * operation error.
 */
public class MongoDBException extends Exception {

    /**
     * A new empty MongoDBException.
     */
    public MongoDBException() {
        super();
    }

    /**
     * A new MongoDBException with a message.
     *
     * @param msg The message to send.
     */
    public MongoDBException(String msg) {
        super(msg);
    }

    /**
     * A new MongoDBException with a message and a throwable.
     *
     * @param msg The message to send.
     * @param throwable The throwable to throw.
     */
    public MongoDBException(String msg, Throwable throwable) {
        super(msg, throwable);
    }
}
