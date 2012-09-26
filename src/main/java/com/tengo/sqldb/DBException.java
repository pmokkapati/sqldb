/*
 * Database exception
 *
 * @author Prasad Mokkapati  prasadm80@gmail.com
 */
package com.tengo.sqldb;
import java.io.Serializable;

public class DBException extends Exception 
        implements Serializable {

    public DBException() {
        this("Unknown Database Exception");
    }
    
    public DBException(final String reason) {
        super(reason);
    }

    public DBException(final Throwable e) {
        super(e);
    }
}
