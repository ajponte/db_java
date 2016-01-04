package me.upalate.db.mongo;

/**
 * Enums representing valid Mongo operations for queries.
 */
public enum MongoOperationType {

    /** Return all elements in the collection. */
    ALL,
    AND,
    EQUALS,
    EXISTS,
    GREATER_THAN,
    GREATER_THAN_OR_EQUALS,
    LESS_THAN,
    LESS_THAN_OR_EQUALS;
}
