
package com.tengo.sqldb;
import java.io.Serializable;

public class DuplicateException extends DBException 
        implements Serializable {

    public DuplicateException() {
        this("Duplicate Exception");
    }
    
    public DuplicateException(final String reason) {
        super(reason);
    }

    public DuplicateException(final Throwable e) {
        super(e);
    }
}
