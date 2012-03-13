/*
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
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.ParameterizedType;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.text.SimpleDateFormat;
import java.util.concurrent.ConcurrentHashMap;

import javax.sql.DataSource;


import com.google.inject.Inject;
import com.google.inject.Singleton;

/**
 * Map of bean properties to database and columns.
 */
public class BeanMap {
    // Map of all beans used by DBManager
    private static ConcurrentHashMap<Type, BeanMap> _maps = 
            new ConcurrentHashMap<Type, BeanMap>();

    private Type _type;
    private Class<?> _class;

    // Map of all properties in this bean
    private HashMap<String, Property> _properties = 
                new HashMap<String, Property>();
    private String _tableName = null;
    private Property _uniqueKeys[] = null;
    private Property _idProp = null;


    // Cache strings useful for insert prepare statement
    private String _insertNames = null; 
    private String _insertValues = null;

    // Cache strings useful for delete and update
    private String _whereStr = null;
    private String _updateNames = null;

    /**
     * Max num of rows in each chunk for bulk inserts
     */
    public static final int BULKINSERT_CHUNK_SIZE=100;


    // List of all primitive properties in this class
    private ArrayList<Property> _primitives = null; 
    // List of all properties that return lists
    private ArrayList<Property> _lists = null;   
    // List keys
    private ArrayList<Property> _listKeys = null;


    // Package private methods used by ColMap
    ArrayList<Property> getPrimitives() { return _primitives; }
    ArrayList<Property> getLists() { return _lists; }
    ArrayList<Property> getListKeys() { return _listKeys; }
    String getTableName() { return _tableName; }
    String getClassName() { return _class.getName(); }


    /**
     *  Create a new instance of map of the given bean type
     *  @param t type of the bean
     */
    private BeanMap(Type t) throws DBException {       
        _type = t;
        // Is this parameterized type ? If so get Raw type
        if ( t instanceof ParameterizedType ) {
            Type rt = ((ParameterizedType)t).getRawType();
            if ( rt instanceof Class) {
                _class = (Class)rt;
            }
        }
        else if ( t instanceof Class) {
            _class = (Class)t;
        }
        else { // Not a bean type
            throw new DBException("BeanMap: Cannot create a mapping for type '"
                    + t + "'");
        }
        PropertyDescriptor props[] = null;
        try {
            props = Introspector.getBeanInfo(_class, Object.class)
                .getPropertyDescriptors();
        }
        catch(IntrospectionException e) {
            throw new DBException("BeanMap: Unable to get Bean Info for "
                + _class.getName());
        }
        initialize(props);
    }
        
    /**
     * Check to see if the given property is a primary key
     */
    private boolean isKey(Property p) {
        if ( _uniqueKeys != null) {
            for (Property kp: _uniqueKeys) {
                if ( p == kp) {
                    return true;
                }
            }
        }
        return false;
    }
    /**
     * Initialize all caches
     */
    private void initialize(PropertyDescriptor props[]) throws DBException {
        // Create a map of properties
        Property pp = null;
        for (PropertyDescriptor d : props) {
            pp = new Property(d, _type); // Create property
            if ( pp.isId() ) {
                if ( _idProp != null) {
                    throw new DBException("Bean '" + _class.getName() + "' "
                        + " has more than one Id Annotation.");
                }
                _idProp = pp;
            }
            _properties.put(pp.getColName(), pp);
        }
        Table table = _class.getAnnotation(Table.class);
        if ( table == null ) { // If no table used for select only
            initSelect();
            return;
        }
        _tableName = table.name().toLowerCase();
        String keys[] = table.uniqueConstraints();
        if ( keys.length > 0) {
            _uniqueKeys = new Property[keys.length];
            StringBuilder whereStr = null;
            String pname = null;
            for (int i=0; i< keys.length; i++) {
                if ( (_uniqueKeys[i] = _properties.get(keys[i].toLowerCase())) 
                        == null) {
                    throw new DBException("Unique key " + keys[i] 
                        + " does not exist in class " + _class.getName());
                }
                if ( whereStr == null ) {
                    whereStr = new StringBuilder(_uniqueKeys[i].getColName() 
                        + "=?");
                }
                else {
                    whereStr.append(" and " + _uniqueKeys[i].getColName() 
                        + "=?");
                }
            }
            _whereStr = (whereStr != null) ? whereStr.toString() : null;
        }
        else if (_idProp != null) {
            _uniqueKeys = new Property[1];
            _uniqueKeys[0] = _idProp;
            _whereStr = _uniqueKeys[0].getColName() + "=?";
        }
        // Cache insert and update strings. Use only primitive
        StringBuilder names = null;
        StringBuilder values = null;
        StringBuilder update = null;
        for (Property p: _properties.values()) {
            if ( !p.isPrimitive() ) {
                continue;
            }
            if ( p.isInsertable() && !p.isId() ) {
                if ( names == null) {
                    names = new StringBuilder(p.getColName());
                    values = new StringBuilder("?");
                }
                else {
                    names.append("," + p.getColName());
                    values.append(",?");
                }
            }
            if ( p.isUpdateble() && !isKey(p) ) {
                if ( update == null) {
                    update= new StringBuilder(p.getColName() + "=?");
                }
                else {
                    update.append("," + p.getColName() + "=?");
                }
            }
        }
        _insertNames= (names==null) ? null : names.toString();
        _insertValues= (values == null) ? null : values.toString();
        _updateNames = (update == null) ? null : update.toString();
        initSelect();
    }

    /**
     * Function to add primitive properties to the given array list
     * @param props array to add this bean primitive properties
     * @param parent parent property in case of nested bean. if null no parent
     */
    private void addPrimitiveProperties(ArrayList<Property> props,
            NestedProperty parent) {
        for (Property m: _properties.values()) {
            if ( !m.isSelectable() ) {
                continue;
            }
            switch (m.getPropertyType()) {
                case Primitive:
                    props.add( (parent == null) ? m 
                        : new NestedProperty(parent, m));
                break;
                case Bean:
                    m.getMap().addPrimitiveProperties(props,
                        (parent == null) ? new NestedProperty(parent)
                        : new NestedProperty(parent, m));
                    break;
                default:
                    break;
            }
        }
    }
    /**
     * Initialize select related cache
     */
    private void initSelect() throws DBException {
        _primitives = new ArrayList<Property>();
        addPrimitiveProperties(_primitives, null);

        String mappedBy[] = null;
        Property key = null;
        for (Property m: _properties.values()) {
            if ( !m.isSelectable() ) {
                continue;
            }
            switch (m.getPropertyType()) {
                case List:
                    if ( _lists == null) {
                        _lists = new ArrayList<Property>();
                    }
                    _lists.add(m);
                    if ( _listKeys == null && 
                                (mappedBy = m.getMappedBy()) != null) {
                        _listKeys = new ArrayList<Property>();
                        for (String s: mappedBy) {
                            if ( (key = _properties.get(s.toLowerCase())) 
                                    == null) {
                                throw new DBException("OneToMany mappedBy '"
                                    + s + "' property does not exist in "
                                    + _class.getName());
                            }
                            _listKeys.add(key);
                        }
                    }
                    break;
                case Bean:
                    if ( _listKeys == null) {
                        // See if the bean contains any lists and if so 
                        // add those keys
                        BeanMap bm = m.getMap();
                        if ( bm._listKeys != null ) {
                            _listKeys = new ArrayList<Property>();
                            for (Property bp: bm._listKeys) {
                                _listKeys.add(new NestedProperty(m, bp));
                            }
                        }
                    }
                    break;
                default:
                    break;
            }
        }
        if ( _listKeys == null && _lists != null ) { // no keys
            throw new DBException(_class.getName() + "."
                + _lists.get(0).getName() + " does not have OneToMany "
                + "annotation");
        }
    }

    /**
     *  Static function that return a class map given a class. This function
     *  is private to the package. Creates one if it was not created earlier.
     *  @param t  the type
     *  @return the BeanMap instance
     */
    static BeanMap get(Type t) throws DBException {
        BeanMap m = _maps.get(t);
        if ( m == null) {
            synchronized(t) {
                if ( (m = _maps.get(t)) == null) {
                    m = new BeanMap(t);
                    _maps.put(t, m);
                }
            }
        }
        return m;
    }
    /**
     * Function to create a new instance of this class
     */
    public Object newInstance() throws DBException {
        try {
            return _class.newInstance();
        }
        catch (Exception e) {
            throw new DBException(e);
        }
    }

    /**
     * Internal function to prepare a statement for single/bulk inserts
     */
    private PreparedStatement prepareInsert(Connection conn, int numRows) 
            throws SQLException, DBException  {
        if ( _insertNames == null || _insertValues == null) {
            throw new DBException("Class " + _class.getName()
                + " does not have any properties to insert");
        }
        StringBuilder s = new StringBuilder("insert into " 
            + _tableName + "(" + _insertNames + ") values ");
        for (int i=0; i < numRows; i++) {
            if ( i > 0 ) {
                s.append(", (" + _insertValues + ")");
            }
            else {
                s.append("(" + _insertValues + ")");
            }
        }
        // If a column is auto increment/serial 
        // if ( _idProp != null && _idProp.isAutoIncrement()) 
        if ( _idProp != null ) {
            // Check to see if auto generated keys are supported
            if ( conn.getMetaData().supportsGetGeneratedKeys() ) {
                return conn.prepareStatement(s.toString(), 
                        Statement.RETURN_GENERATED_KEYS);
            }
            else { // Assume postgresql
                return conn.prepareStatement(s.toString() + " returning "
                    + _idProp.getColName());
            }
        }
        else {
            return conn.prepareStatement(s.toString());
        }
    }


    /**
     * Internal function to set values on a prepared statement so it can be
     * executed. 
     */
    private int setInsertValues(DBManager pmgr, Connection conn, 
            PreparedStatement stmt, Object o, int idx) 
                    throws DBException, SQLException {
        for (Property m: _properties.values()) {
            if ( !m.isPrimitive() ||  !m.isInsertable() || m.isId()) {
                continue;
            }
            m.setValue(o, stmt, ++idx);
        }
        return idx;
    }

    /**
     * Function to insert object into database. Mutiple nested tables inserts 
     * are handled as well. If beans have serial fields, the bean serial
     * property is updated to new value as well.
     */
    public int insert(DBManager pmgr, Connection conn, Object obj) 
            throws SQLException, DBException {
        if ( _tableName == null) {
            throw new DBException(
                "Class " + _class.getName() + " does not have Table "
                + "annotation needed for insert. ");
        }
        PreparedStatement stmt = null;
        try {
            stmt = prepareInsert(conn, 1);
            setInsertValues(pmgr, conn, stmt, obj, 0);
            int ret=0;
            if ( _idProp != null ) {
                ResultSet res;
                // Check to see if auto generated keys are supported
                if ( conn.getMetaData().supportsGetGeneratedKeys() ) {
                    ret = stmt.executeUpdate();
                    res = stmt.getGeneratedKeys();
                }
                else { // Must be using select returning ..
                    res = stmt.executeQuery();
                    ret = 1;
                }
                if ( res.next() ) {
                    _idProp.setValue(obj, res.getLong(1));
                }
            }
            else { 
                ret = stmt.executeUpdate();
            }
            return ret;
        }
        finally {
            if ( stmt != null) {
                stmt.close();
            }
        }
    }
    /**
     * Function to do bulk insert. Insert in chunks
     */
    public int bulkInsert(DBManager pmgr, Connection conn, 
                List list) throws SQLException, DBException {
        if ( _tableName == null) {
            throw new DBException(
                "Class " + _class.getName() + " does not have DBTableMap "
                + "annotation ");
        }
        PreparedStatement stmt = null;
        int size = list.size();
        int ret = 0;
        int cur =0;
        int rows;
        try {
            while ( size > 0 ) {
                rows = (size < BULKINSERT_CHUNK_SIZE) ? size 
                        : BULKINSERT_CHUNK_SIZE;
                stmt = prepareInsert(conn, rows);
                int idx = 0;
                for (int i=0; i < rows; i++) {
                    idx = setInsertValues(pmgr, conn, stmt, list.get(cur++),
                        idx);
                }
                if ( _idProp != null ) {
                    ResultSet res;
                    // Check to see if auto generated keys are supported
                    if ( conn.getMetaData().supportsGetGeneratedKeys() ) {
                        stmt.executeUpdate();
                        res = stmt.getGeneratedKeys();
                    }
                    else { // Must be using select returning ..
                        res = stmt.executeQuery();
                    }
                    int sidx = cur - rows;
                    Object obj = null;
                    while ( res.next() ) {
                        ret++;
                        obj = list.get(sidx++);
                        _idProp.setValue(obj, res.getLong(1));
                    }
                }
                else {
                    ret += stmt.executeUpdate();
                }
                size -= rows;
            }
            return ret;
        }
        finally {
            if ( stmt != null) {
                stmt.close();
            }
        }
    } 
    /**
     * Function to delete an object
     */
    public int delete(DBManager pmgr, Connection conn, Object obj) 
            throws SQLException, DBException {
        if ( _tableName == null) {
            throw new DBException(
                "Class " + _class.getName() + " does not have DBTableMap "
                + "annotation ");
        }
        if ( _whereStr == null) {
            throw new DBException("Class " + _class.getName()
                + " does not have properties to update");
        }
        PreparedStatement stmt = null;
        try {
            stmt= conn.prepareStatement("delete from " 
                    + _tableName + " where " + _whereStr);
            int idx=0;
            for (Property p: _uniqueKeys) {
                p.setValue(obj, stmt, ++idx);
            }
            return stmt.executeUpdate();
        }
        finally {
            if ( stmt != null) {
                stmt.close();
            }
        }
    }

            
    /**
     * Function to select records from ResultSet and insert into the given
     * list
     * @param res Result of query
     * @param list Array list to populate
     */
    public void select(ResultSet res, ArrayList list) throws SQLException, 
            DBException {
        ColMap map = new ColMap(this, res);
            
        Object prev=null;
        Object obj;
        while (res.next() ) {
            if ( (obj = map.mapValues(prev, res)) != prev) {
                list.add(obj);
            }
            prev = obj;
        }
    }
    /**
     * Function to retrieve an object
     */
    public Object get(ResultSet res, Object orig)
            throws SQLException, DBException {
        ColMap map = new ColMap(this, res);

        if ( orig == null) {
            if ( res.next() ) {
                orig = map.mapValues(orig, res);
            }
        }
        Object cur = orig;
        while (res.next() ) {
            if ( map.mapValues(cur, res) != orig) {
                return orig;
            }
        }
        return orig;
    }
    /**
     * Function to retrieve an Object based on the keys of an object
     */
    public Object get(DBManager pmgr, Connection conn, Object obj) 
                    throws SQLException, DBException {
        if ( _tableName == null ) {
            throw new DBException("DBTableMap annotation is required "
                + "in a Bean to build a select query");
        }
        PreparedStatement stmt = null;
        ResultSet res = null;
        try {
            stmt = conn.prepareStatement( 
                "select * from " + _tableName + " where " + _whereStr);
            int idx = 0;
            // Set key values
            for (Property p: _uniqueKeys) {
                p.setValue(obj, stmt, ++idx);
            }
            res = stmt.executeQuery();
            Object ret = get(res, null);
            return (ret == null ? obj : ret);
        }
        finally {
            if ( stmt != null ) {
                stmt.close();
            }
            if ( res != null ) {
                res.close();
            }
        }
    }

    /**
     * Internal function to set update values on a prepared statement so 
     * it can be executed. TBD- Should we update nested tables as well ?
     */
    private int setUpdateValues(DBManager pmgr, Connection conn, 
            PreparedStatement stmt, Object o, int idx) 
                    throws DBException, SQLException {
        for (Property m: _primitives) {
            if ( isKey(m) || ! m.isUpdateble() ) {
                continue;
            }
            m.setValue(o, stmt, ++idx);
        }
        // Set key values
        if ( _uniqueKeys != null ) {
            for (Property p: _uniqueKeys) {
                p.setValue(o, stmt, ++idx);
            }
        }
        return idx;
    }
    /**
     * Function to update an object. TBD - Update Nested tables ?
     *
     */
    public int update(DBManager pmgr, Connection conn, Object obj) 
            throws SQLException, DBException {
        if ( _tableName == null) {
            throw new DBException(
                "Class " + _class.getName() + " does not have DBTableMap "
                + "annotation ");
        }
        if ( _updateNames == null ) {
            throw new DBException(
                "Class " + _class.getName() + " does not have any properties "
                    + " to update");
        }
        PreparedStatement stmt = null;
        try {
            stmt = conn.prepareStatement( 
                "update " + _tableName + " set " + _updateNames  
                + " where " + _whereStr);
            setUpdateValues(pmgr, conn, stmt, obj, 0);
            return stmt.executeUpdate();
        }
        finally {
            if ( stmt != null ) {
                stmt.close();
            }
        }
    }
}
