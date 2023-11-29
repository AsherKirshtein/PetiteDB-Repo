package edu.yu.dbimpl.tx.recovery;

public enum LogRecordType 
{
    COMMIT,
    SET_INT,
    SET_DOUBLE,
    SET_BOOLEAN,
    SET_STRING,
    ROLLBACK
}
