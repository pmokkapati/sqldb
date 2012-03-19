/*
 * 
 * Copyright 2012 by Tengo, Inc.
 * All rights reserved.
 *
 * This software is the confidential and proprietary information 
 * of Tengo, Inc.
 *
 * @author psm
 */
package com.tengo.sqldb;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.text.SimpleDateFormat;
import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;

import com.google.inject.Inject;
import com.google.inject.Singleton;

/**
 * Class to map relational database tables, views .. to java beans.
 * The model used here is a simple relational database mapping and not a 
 * persistence model like java persistence, hibernate etc. without all the 
 * complexities associated with java persistence.
 * The assumes that the underneath database is a relational datbase and simply
 * provides a mechanism to map relational database tables/views to java beans.
 * There will be one instance of DBManager for each different datasource
 */

public class DBManager {

    private static final SimpleDateFormat _dformat =
            new SimpleDateFormat("yyyy-MM-dd");

    private final DataSource _dataSource;
    private final ThreadLocal<Connection> _connection = 
            new ThreadLocal<Connection>();

    private static final ThreadLocal<Integer> _transactionalCnt =
            new ThreadLocal<Integer>();

    // List of DBManagers in a thread that are in a Transactional block
    private static final ThreadLocal<ArrayList<DBManager>> _dbManagers=
            new ThreadLocal<ArrayList<DBManager>>();

    /* 
     * Constructor needed for Guice
     */
    @Inject
    private DBManager(DataSource source) {
	this._dataSource = source;
    }

    /**
     * package private function used by Transactional interceptors to 
     * begin and end transactions. 
     */
    static boolean beginTransaction() {
        Integer cnt = _transactionalCnt.get();
        if ( cnt == null ) { // No transactional call yet
            cnt = new Integer(1);
            _transactionalCnt.set(cnt);
            return true;
        }
        else {
            return false;
        }
    }

    static void commitTransaction() throws DBException {
        Integer cnt = _transactionalCnt.get();
        if ( cnt == null) {
            throw new DBException("No Transaction to commit");
        }
        else {
            // Commit all the DBManagers in thread
            ArrayList<DBManager> mgrs = _dbManagers.get();
            if ( mgrs != null) {
                for (DBManager mgr: mgrs) {
                    mgr.commit();
                }
                _dbManagers.set(null);
            }
            _transactionalCnt.set(null);
        }
    }
    static void rollbackTransaction() throws DBException {
        ArrayList<DBManager> mgrs = _dbManagers.get();
        if ( mgrs != null) {
            for (DBManager mgr: mgrs) {
                mgr.rollback();
            }
            _dbManagers.set(null);
        }
        _transactionalCnt.set(null);
    }

    /**
     * Function to check if the current thread is in a Transactional context
     */
    static boolean inTransaction() {
        return (_transactionalCnt.get() != null);
    }

    /* 
     * Object cannot be created without data source
     */
    private DBManager() {
	this._dataSource=null;
    }

    /**
     * Starts a transaction and attaches the connection thread local storage
     */
    public void begin() throws DBException {
	if (_connection.get() != null) {
            throw new DBException(
                "DB Connection already set in current thread");
        }
        begin(getConnection());
    }


    private void begin(Connection c) throws DBException {
        try {
            c.setAutoCommit(false);
            _connection.set(c);
        } 
        catch (SQLException e) {
            handleException(e,"",null);
        }
    }
    
    /**
     * Commit a transaction.
     */
    public void commit() throws  DBException {
        Connection c = _connection.get();
	if (c == null) {
            throw new DBException("DB Connection is not set in current thread");
        }
        try {
            c.commit();
            
        } catch (SQLException e) {
            handleException(e,"",null);
        }
        finally {
            _connection.set(null);
	    release(c);
        }
    }

    /**
     * Function to rollback a transaction
     */
    public void rollback() throws  DBException {
	Connection c = _connection.get();
	if (c == null) {
            throw new DBException("DB Connection is not set in current thread");
        }
        try {
            c.rollback();
        } catch (SQLException e) {
            handleException(e,"",null);
        }
        finally {
	    _connection.set(null);
	    release(c);
        }
    }


    /**
     * Function to get a pooled connection. If a connection was retrieved
     * before and kept in threadlocal connection return it otherwise get a 
     * new connection from data source
     */
    protected Connection getConnection() throws DBException {
        try {
            Connection c = _connection.get();
            if (c == null) { // No connections in this thread. 
                c = _dataSource.getConnection();
                if ( inTransaction() ) {
                    begin(c);
                    _connection.set(c);
                    ArrayList<DBManager> mgrs = _dbManagers.get();
                    if ( mgrs == null) {
                        mgrs = new ArrayList<DBManager>();
                        _dbManagers.set(mgrs);
                    }
                    mgrs.add(this);
                }
            }
            return c;
        }
        catch (Exception e) {
            throw new DBException(e);
        }
    }

    /**
     * Function to release a connection. If this connection to be released
     * is also stored in thread local - I.E. connection created by a external
     * begin transaction, then do not close connection
     */
    protected void release(Connection c) throws DBException {
        try {
            Connection tc = _connection.get();
            if ( tc != c) {
                c.close();
            }
        } 
        catch (SQLException e) {
            throw new DBException(e);
        }
    }

    private void handleException(Exception ex, String className,
                Connection conn) throws  DBException {
        if ( ex instanceof java.net.SocketException ||
             ex.toString().indexOf("java.net.SocketException") > -1  ||
             ex.toString().indexOf("broken the connection") > -1 ) {
             if ( conn != null) {
                 close(conn);  
            }
             throw new DBException("");
        } 
        if (ex instanceof IllegalAccessException ) {
            DBException pex = new DBException(
                "Can't create object " + className
                + ", no public default constructor or class not accessible");
            pex.initCause(ex);
            throw pex;
        }
        else if (ex instanceof InstantiationException) {
            DBException pex = new DBException(
                "Can't create object " + className
                + ", no public default constructor or invalid class type");
            pex.initCause(ex);
            throw pex;
        }
        else if (ex instanceof java.sql.SQLIntegrityConstraintViolationException) {
            // Assuming unique key violation since we do not use foriegn keys
            throw new DuplicateException(ex);
        }
	else if (ex instanceof DBException) {
	    throw (DBException) ex;
	}
        else {
            if ( !(ex instanceof SQLException) ) {
                ex.printStackTrace();
            }
            else { // Fix for postgres. Does not throw SQLIntegrityConstraint..
                   // Exception. So have to look at message to determine dup.
                String err= ex.getMessage();
                if ( err.matches(".*duplicate key (value ){0,1}violates unique constraint.*")) {
                    throw new DuplicateException(ex);
                }
            }
            DBException pex = 
		new DBException("Can't create object " 
                    + className + ": " + ex.getMessage());
            pex.initCause(ex);
            throw pex;
        }
    }
    /**
     *  Function to execute a sql statement - normally used for 
     *  update/delete/insert that return nothing (not queries)
     *  @param  str statement to execute
     *  @return the number of rows update/deleted/inserted or zero for 
     *  statements ret
     *  @throws SQLException
     */
    public int executeSQL(String str) throws  DBException {
        Connection conn = getConnection();
        Statement stmt = null;
        try {
            stmt = createStatement(conn);
            return stmt.executeUpdate(str);
        }
        catch (Exception ex) {
            handleException(ex, "executeSQL: "+ str,conn);
        }
        finally {
            close(stmt);
            release(conn);
        }
        return 0;
    }

    /**
     *  Delete Objects from Database
     *  @param  o object to be deleted
     *  @return the number of rows deleted    
     *  @throws DBException
     */
    public <T> int delete(T o) throws  DBException {
        BeanMap map = BeanMap.get(o.getClass());
        Connection conn = getConnection();
        try {
            return map.delete(this, conn, o);
        }
        catch (Exception ex) {
            handleException(ex, o.getClass().getName(),conn);
        }
        finally {
            release(conn);
        }
        return 0;
    }
  

    /**
     * Inserts an object  into database
     * @param o Object to be inserted
     * @return 1 if insert succeeded else returns 0
     */
    public <T> int insert(T o) throws  DBException {
        BeanMap map = BeanMap.get(o.getClass());
        Connection conn = getConnection();
        try {
            return map.insert(this, conn, o);
        }
        catch (Exception ex) {
            handleException(ex, o.getClass().getName(),conn);
        }
        finally {
            release(conn);
        }
        return 0;
    }
    /**
     * Inserts a list of objects into a database
     * @param l list of objects to be inserted
     * @return number of inserts 
     */
    public <T> int bulkInsert(List<T> l) throws  DBException {
        if (l.size() < 1) {
            return 0;
        }
        BeanMap map = BeanMap.get(l.get(0).getClass());
        Connection conn = getConnection();
        try {
            return map.bulkInsert(this, conn, l);
        }
        catch (Exception ex) {
            handleException(ex, l.get(0).getClass().getName(),conn);
        }
        finally {
            release(conn);
        }
        return 0;
    }
    /**
     * Update an object  into database
     * @param o Object to be updated
     * @return number of rows if update succeeded else returns 0
     */
    public <T> int update(T o) throws  DBException {
        BeanMap map = BeanMap.get(o.getClass());
        Connection conn = getConnection();
        try {
            return map.update(this, conn, o);
        }
        catch (Exception ex) {
            handleException(ex, o.getClass().getName(),conn);
        }
        finally {
            release(conn);
        }
        return 0;
    }
    /**
     *  Retrieves a single row from the database
     *
     *  @param  o that contains the keys 
     *  @return  the same object passed int
     *         
     *  @throws DBException
     */
    @SuppressWarnings(value="unchecked")
    public <T> T get(T o) throws  DBException {
        BeanMap map = BeanMap.get(o.getClass());
        Connection conn = getConnection();
        try {
            return (T)map.get(this, conn, o);
        }
        catch (Exception ex) {
            handleException(ex, map.getClass().getName(),conn);
            return null;
        }
        finally {
            release(conn);
        }
    }
    /**
     *  Retrieves a single row from the database
     *
     *  @param  c Class to map the query row to
     *  @param  query sql query
     *  @return new Object
     *         
     *  @throws DBException
     */
    @SuppressWarnings(value="unchecked")
    public <T> T get(Class<T> c, String query) throws  DBException {
        BeanMap map = BeanMap.get(c);
        Connection conn = getConnection();
        Statement stmt = null;
        ResultSet res = null;
        boolean isPrimitive = c.isPrimitive();
        try {
            stmt = createStatement(conn);
            res = stmt.executeQuery(query);
            if (isPrimitive ) {
                if ( res.next()) {
                    return (T)res.getObject(1);
                }
                else {
                    return null;
                }
            }
            else {
                    return (T)map.get(res, (Object)null);
            }
        }
        catch (Exception ex) {
            handleException(ex, c.getName(),conn);
            return null;
        }
        finally {
            close(res);
            close(stmt);
            release(conn);
        }
    }
    /**
     *  Retrieves a set of rows from database based on the query. 
     *
     *  @param  c Class to map the query rows to
     *  @param  query sql query
     *  @return list of Objects of type T
     *         
     *  @throws DBException
     */
    public <T> ArrayList<T> select(Class<T> c, String query)
            throws  DBException {
        Connection conn = getConnection();
        Statement stmt = null;
        ArrayList<T> ret = new ArrayList<T>();
        T obj=null;
        ResultSet res = null;
        BeanMap map = BeanMap.get(c);
        boolean isPrimitive = c.isPrimitive();
        try {
            stmt = createStatement(conn);
            res = stmt.executeQuery(query);
            if ( isPrimitive) {
                while (res.next() ) {
                    ret.add((T)(res.getObject(1)));
                }
            }
            else {
                map.select(res, ret);
            }
            return ret;
        }
        catch (Exception ex) {
            handleException(ex, c.getName(),conn);
            return ret;
        }
        finally {
            close(res);
            close(stmt);
            release(conn);
        }
    }
    
    /**
     * Static function to format Date for sql statements. Note: No quotes added
     * @param d to be formatted
     * @return formatted string - format YYYY-MM-DD
     */
    public static String sqlDate(java.util.Date d) {
        return _dformat.format(d);
    }
    

    private Statement createStatement(Connection connection) 
            throws DBException {
        try {
            return connection.createStatement();
        } catch (SQLException e) {
            handleException(e,"",null);
        }
        return null;
    }
    



    private void close(Connection connection) throws DBException {
        try {
            connection.close();
        } catch (SQLException e) {
            handleException(e,"",null);
        }
        
    }

    private void close(Statement stmt) throws DBException {
        try {
            stmt.close();
        } catch (SQLException e) {
            handleException(e,"",null);
        }
    }
 
    private void close(ResultSet res) throws DBException {
        try {
            if (res !=null) {
                res.close();
            }
        } catch (SQLException e) {
            handleException(e,"",null);
        }
    }
}
