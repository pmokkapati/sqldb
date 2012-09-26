/*
 * Class to store information about a bean property
 *
 * @author Prasad Mokkapati  prasadm80@gmail.com
 */
package com.tengo.sqldb;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.InvocationTargetException;
import java.lang.annotation.Annotation;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.Types;

import java.util.List;
import java.util.Map;

/**
 * Class to store a bean property and related database mapping.
 */
public class Property {
    public enum PropertyType { Unknown, Primitive, Bean, List, Nested};
    private PropertyType _type = PropertyType.Unknown;
    private PropertyDescriptor _desc=null;
    private BeanMap _map = null;
    private String _colName = null;
    private Column _colAnn = null;
    private OneToMany _oneToManyAnn = null;
    private Enumerated _enum = null;
    private boolean _isId = false;
    private Class _class=null;

    private static class SQLMap {
        private Class _c;
        private int _type;

        public SQLMap(Class c, int t) {
            _c = c;
            _type = t;
        }
        public int getType() {
            return _type;
        }
        public boolean matches(Class c) {
            return (_c == c);
        }
    }


    // Map of java types to SQL types
    private static SQLMap _sqlTypeMap[] = {
        new SQLMap(Integer.class, Types.INTEGER),
        new SQLMap(Short.class, Types.INTEGER),
        new SQLMap(Long.class, Types.BIGINT),
        new SQLMap(Float.class, Types.FLOAT),
        new SQLMap(Double.class, Types.DOUBLE),
        new SQLMap(Character.class, Types.CHAR),
        new SQLMap(Byte.class, Types.BIT),
        new SQLMap(Boolean.class, Types.BOOLEAN),
        new SQLMap(java.util.Date.class, Types.DATE),
        new SQLMap(java.sql.Date.class, Types.DATE),
        new SQLMap(java.sql.Time.class, Types.TIME),
        new SQLMap(java.sql.Timestamp.class, Types.TIMESTAMP),
        new SQLMap(String.class, Types.VARCHAR)
    };

    protected static Object getEnum(Class cl, int ordinal) {
        Object val[] = cl.getEnumConstants();
        if ( ordinal < 0 || ordinal >= val.length) {
            return null;
        }
        else {
            return val[ordinal];
        }
    }

    /**
     * Method to return SQL type mapping for java "primitive" classes
     * @param c class to get mapping for
     * @return returns the sql type. If mapping does not exist return -1
     */
    protected static int getSqlType(Class c) {
        for (SQLMap  m: _sqlTypeMap) {
            if ( m.matches(c) ) {
                return m.getType();
            }
        }
        return -1;
    }
    /**
     * Method to determine if a class is "primitive"
     * @param c class to get mapping for
     * @return true is primitive, false otherwise
     */
    protected static boolean isPrimitive(Class c) {
        return (c.isEnum() || c.isPrimitive() || getSqlType(c) != -1 );
    }


    /** 
     * Internal function to get an annotation if it exists on the property - 
     * I.E. on a get/set method
     * @param ac The annotation class
     * @return the annotation if it exists else null
     */    
     private  <T extends Annotation> T getAnnotation(Class<T> ac) {
        T a = _desc.getReadMethod().getAnnotation(ac);
        return (a == null) ? _desc.getWriteMethod().getAnnotation(ac) : a;
    }

    /** 
     * Internal function to intialize all known annotations on the property 
     * I.E. on a get/set method
     */    
     private void initAnnotations() {
        _colAnn = getAnnotation(Column.class);
        _oneToManyAnn = getAnnotation(OneToMany.class);
        _isId = (getAnnotation(Id.class) != null);
        _enum =getAnnotation(Enumerated.class);
    }

    protected Property(PropertyType t) {
        _type =t;
    }
            
    /**
     * Constructor
     * Create a property from a property descriptor
     * @params p property descriptor
     * @throws DBException if property type is not supported
     */
    public Property(PropertyDescriptor p, Type enclosing) throws DBException {
        Method getter = p.getReadMethod();
        _desc = p;
        initAnnotations();
        initType(getter.getGenericReturnType(), enclosing);
        if ( _oneToManyAnn != null && _type != PropertyType.List) {
            // OneToMany needs to return a list.
            throw new DBException("Invalid OneToMany annotation on "
                + getter.getDeclaringClass().getName()
                + ":" + _desc.getName() + ". Property needs to be a list type");
        }
        // Check to see if column annotation exists if, so use that name
        if ( _colAnn != null && !_colAnn.equals("") ) {
            _colName = _colAnn.name();
        }
        else {
            _colName = p.getName().toLowerCase();
        }
    }

    /**
     * Internal function to initialize the type
     */
    private void initType(Type t, Type enclosing) throws DBException {
        if ( t instanceof Class && isPrimitive((Class)t) ) {
            _type = PropertyType.Primitive;
            _class = (Class)t;
        }
        else if ( t instanceof ParameterizedType) { // See if type is list
            ParameterizedType pt = (ParameterizedType)t;
            Type rt = pt.getRawType(); // See if type is class
            if ( rt instanceof Class 
                        && java.util.List.class.isAssignableFrom((Class)rt)) {
                Type listType = pt.getActualTypeArguments()[0];
                if ( listType instanceof Class && 
                                isPrimitive((Class)listType) ) {
                    throw new DBException(_desc.getName() 
                        + " property on class "
                        + _desc.getReadMethod().getDeclaringClass().getName()
                        +  " is list of primitives and is not supported");
                } 
                _map = BeanMap.get(listType);
                _type = PropertyType.List;
                 _class = (Class)rt;
            }
            else {
                _type = PropertyType.Bean;
                _map = BeanMap.get(t);
                _class = _desc.getPropertyType();
            }
        }
        else if ( t instanceof TypeVariable) {
            // Parameterized bean. So need to get the instantiated type
            TypeVariable vt = (TypeVariable)t;
            Class c = _desc.getReadMethod().getDeclaringClass();
            if ( ! (enclosing instanceof ParameterizedType) ) {
                throw new DBException("Error mapping parameterized property " 
                    +  _desc.getName() + " in class '" + c.getName() + "'");
            }
            ParameterizedType pt = (ParameterizedType)enclosing;
            TypeVariable param[] = c.getTypeParameters();
            for (int i=0; i < param.length; i++) {
                if ( param[i].getName().equals(vt.getName()) ) {
                    initType(pt.getActualTypeArguments()[i], enclosing);
                    return;
                }
            }
        }
        else if ( t instanceof Class) {
            _type = PropertyType.Bean;
            _map = BeanMap.get(t);
            _class = _desc.getPropertyType();
        }
    }

    /**
     * Function to return the value of this property 
     * @param o Object from which to get the value from
     * @return returns the value object
     */
    public Object getValue(Object o) throws DBException {
        try {
            return _desc.getReadMethod().invoke(o);
        }
        catch (Exception e) {
            throw new DBException(e);
        }
    }
    /**
     * Function to set the value for this property in the given object
     * @param o Object/bean into which the value needs to be set
     * @param v value for this property
     */
    public void setValue(Object o, Object v) throws DBException {
        try {
            _desc.getWriteMethod().invoke(o, v);
        }
        catch (Exception e) {
            System.out.println(o.getClass().getName() 
                + "." + getName() + " type: " + _class.getName()
                + " value type: " + v.getClass().getName());
            throw new DBException(e);
        }
    }
    /**
     * Method to get the property value from the passed object and set it into
     * the prepared statement
     * @param obj The object/bean to retrieve the property value from
     * @param stmt SQL statement to insert value into
     * @param idx the index of argument to set value to
     * @throws DBexception if property is not primitive or cannot be mapped
     *  to a SQL type.
     * @throws SQLException on any SQL errors
     */
    public void setValue(Object obj, PreparedStatement stmt, int idx) 
                throws DBException, SQLException {
        Object v = getValue(obj);
        if ( v == null) {
            int t;
            if ( (t = getSqlType(_desc.getPropertyType())) != -1 ) {
                stmt.setNull(idx, t);
            }
            else {
                System.out.println(v.getClass().getName() 
                    + " cannot be mapped to sql type");
                throw new DBException(v.getClass().getName() 
                    + " cannot be mapped to sql type");
            }
        }
        else if ( _class == java.util.Date.class ||
                _class == java.sql.Date.class) {
            stmt.setObject(idx, v, Types.DATE);
        }
        else if ( _class.isEnum() && v instanceof Enum) {
            if ( _enum == null || _enum.value()==Enumerated.EnumType.ORDINAL) {
                stmt.setInt(idx, ((Enum)v).ordinal());
            }
            else {
                stmt.setObject(idx, v.toString());
            }
        }
        else {
            stmt.setObject(idx, v);
        }
    }
    /**
     * Method to get this property value from a SQL result set
     * @param res SQL resultset
     * @param idx index to retrieve value from
     * @return the object at the given index
     * @throws DBException if this property is not a primitive property
     * @throws SQLException if any SQL errors occur
     */
    public Object getValue(ResultSet res, int idx) 
                throws DBException, SQLException {
        Object ret = res.getObject(idx);
        if ( _class.isEnum() ) {
            if ( _enum == null || _enum.value()==Enumerated.EnumType.ORDINAL) {
                return getEnum(_class, ((Integer)ret).intValue());
            }
            else {
                return Enum.valueOf(_class, (String)ret);
            }
        }
        else {
            return ret;
        }
    }
    /**
     * Method to read a value from ResultSet and set it into the object
     * @param res SQL resultset
     * @param idx index to retrieve value from
     * @return the object at the given index
     * @throws DBException if this property is not a primitive property
     * @throws SQLException if any SQL errors occur
     */
    public void setValue(Object obj, ResultSet res, int idx) 
                throws DBException, SQLException {
        setValue(obj, getValue(res, idx));
    }
    /**
     * Create a new instance of the property type. If type is a list
     * then an empty ArrayList is returned. Otherwise an instance of the 
     * property type is returned
     * @return new instance of property type
     */
    public Object newInstance() throws DBException {
        try {
            return _class.newInstance();
        }
        catch(Exception e) {
            throw new DBException(e);
        }
    }

    /**
     * Function to return the database column name for this property
     * @return database table column name this property is mapped to
     */
    public String getColName() { return _colName; }

    /**
     * Function to return the property name
     * @return property name
     */
    public String getName() { return _desc.getName(); }
    /**
     * Function to check and see if this property is used during an insert
     * @return true if property is used during insert, false otherwise.
     */
    public boolean isInsertable() { 
        return (_colAnn == null || _colAnn.insertable());
    }
    /**
     * Function to check and see if this property is used during an update
     * @return true if property is used during update, false otherwise.
     */
    public boolean isUpdateble() { 
        return (_colAnn == null || _colAnn.updateble());
    }
    /**
     * Function to check and see if this property is used during a select
     * @return true if property is used during select, false otherwise.
     */
    public boolean isSelectable() { 
        return (_colAnn == null || _colAnn.selectable());
    }
    /**
     * Function to check and see if this property is an Id.
     * @return true if primary key id else false.
     */
    public boolean isId() {
        return _isId;
    }
    /**
     * Function to check and see if this property is primitive
     * @return true if true else false
     */
    public boolean isPrimitive() {
        return (_type == PropertyType.Primitive);
    }

    /**
     * Function to check and see if this property is a list
     * @return true if true else false
     */
    public boolean isList() {
        return (_type == PropertyType.List);
    }

    /**
     * Function to return the property type
     * @return the property type
     */
    public PropertyType getPropertyType() {
        return _type;
    }

    /**
     * Function to return the BeanMap for a list or beantype
     * @return the property type
     */
    public BeanMap getMap() {
        return _map;
    }

    public String[] getMappedBy() {
        return (_oneToManyAnn == null) ? null : _oneToManyAnn.mappedBy();
    }
}
