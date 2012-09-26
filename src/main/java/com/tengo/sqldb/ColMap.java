/*
 * Map of bean properties to database columns
 *
 * @author Prasad Mokkapati  prasadm80@gmail.com
 */
package com.tengo.sqldb;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Arrays;

/**
 * Class to store the map of properties to column ids to enable mapping of 
 * data base result set to bean properties
 */
class ColMap {
    private int _colId[] = null; // Column ids of primitives
    private int _listKeyId[] = null;
    private ColMap  _lists[] = null;
    private BeanMap _map = null;

    private ColMap() {}

    /**
     * Internal function to append table name to column if exists. Otherwise
     * throws an exception
     */
    private String appendTableName(ResultSetMetaData md, String colName,
                int colNum) throws SQLException {
        String tableName = md.getTableName(colNum);
        if ( tableName == null) {
            System.out.println("Column " + colName 
                + " does not have a table name ");
            return colName;
        }
        else {
            return  tableName.toLowerCase() + "." + colName;
        }
    }
    /**
     * Internal function to return an error if unable to map all the columns
     * from ResultSet
     * @param res ResultSet
     * @return error string
     */
    private String getColMapError(ResultSetMetaData md, boolean mapped[]) 
                throws SQLException, DBException {
        int ncols = md.getColumnCount();
        StringBuilder s = null;
        for (int i=0; i < ncols; i++) {
            if ( !mapped[i]) {
                String colname = md.getColumnLabel(i+1);
                colname = (colname == null) ? 
                    md.getColumnName(i+1).toLowerCase()
                    : colname.toLowerCase();
                String ts= appendTableName(md, colname, i+1);
                if ( s == null) {
                    s = new StringBuilder(
                        "Mapping error mapping database columns (" + ts);
                }
                else {
                    s.append("," + ts);
                }
            }
        }
        return (s != null ? (s.toString() + ") to " + _map.getClassName() )
            : null);
    }
    /**
     * Map the colids to given array
     */
    private void mapProperties(ArrayList<Property> properties, 
            HashMap<String, Integer> colMap, 
            int colId[], boolean mapped[]) {
        Integer id = null;
        Property p = null;
        String tableName = _map.getTableName();
        for (int i=0; i < colId.length; i++) {
            p = properties.get(i);
            if ( (tableName != null && 
                    (id = colMap.get(tableName + "." + p.getColName())) != null)
                    || (id = colMap.get(p.getColName())) != null) {
                colId[i] = id.intValue();
                mapped[colId[i]-1] = true;
            }
            else {
                //System.out.println("Unable to Map '" + p.getColName() + "'");
                _colId[i] = 0;
            }
        }
    }

    /**
     * Map the lists
     */
    private void mapLists(ArrayList<Property> lists,
            boolean mapped[], HashMap<String, Integer> colMap) {
        _lists = new ColMap[lists.size()];
        for (int i=0; i < _lists.length; i++) {
            _lists[i] = new ColMap();
            _lists[i].init(lists.get(i).getMap(), mapped, colMap);
        }
    }
    /**
     * Initialize mappings for primitive and lists
     */
    private void init(BeanMap m, boolean mapped[], 
                HashMap<String, Integer> colMap) {
        _map = m;
        ArrayList<Property> prop = m.getPrimitives();
        if ( prop != null ) {
            _colId = new int[prop.size()];
            mapProperties(prop, colMap, _colId, mapped);
        }
        prop = m.getListKeys();
        if ( prop != null ) {
            _listKeyId = new int[prop.size()];
            mapProperties(prop, colMap, _listKeyId, mapped);
        }
        prop = m.getLists();
        if ( prop != null && prop.size() > 0) {
            mapLists(prop, mapped, colMap);
        }
    }
        

    private ColMap(BeanMap m, boolean mapped[], 
            HashMap<String, Integer> colMap) throws DBException {
        init(m, mapped, colMap);
    }

    /**
     * package private constructor
     */
    ColMap(BeanMap m, ResultSet res) throws SQLException, DBException {
        ResultSetMetaData md = res.getMetaData();
        HashMap<String, Integer> colMap = new HashMap<String, Integer>();
        int numCol = md.getColumnCount();
        Integer value = null;
        for (int i=1; i <= numCol; i++) {
            String colname = md.getColumnLabel(i);
            colname = colname == null ? md.getColumnName(i).toLowerCase()
                    : colname.toLowerCase();
            // In case of duplicate column names, use table name as well
            // to resolve ambiguities
            if ( (value = colMap.get(colname)) != null) {
                colMap.remove(colname);
                colMap.put(appendTableName(md, colname, value.intValue()),
                    value);
                colMap.put(appendTableName(md, colname, i), new Integer(i));
            }
            else {
                colMap.put(colname, new Integer(i));
            }
        }
        boolean mapped[] = new boolean[colMap.size()];
        Arrays.fill(mapped, false);
        init(m, mapped, colMap);
        String err = getColMapError(md, mapped);
        if (err != null) {
            throw new DBException(err);
        }
    }

    /**
     * Function to check if the keys in the given object are equal to 
     * keys from the current resultset row.
     */
    private boolean keysAreEqual(Object obj, ResultSet res) 
            throws DBException, SQLException {
        ArrayList<Property> listKeys = _map.getListKeys();
        if ( listKeys == null || listKeys.size() == 0) { // No keys
            return false;
        }
        Object objValue = null;
        Object resValue = null;
        for (int i=0; i < _listKeyId.length; i++) {
            objValue = listKeys.get(i).getValue(obj);
            resValue = res.getObject(_listKeyId[i]);
            if (objValue == null || resValue == null) {
                if ( objValue != resValue) {
                    return false;
                }
            }
            else if ( !objValue.equals(resValue) ) {
                return false;
            }
        }
        return true;
    }

    /**
     * Map values from database resultset.
     * @param obj The object to map database row. If obj is null (top level)
     *          then a new instance is created.
     * @param res Database resultset
     * @return the mapped object. 
     */
    Object mapValues(Object obj, ResultSet res)
                throws DBException, SQLException {
        ArrayList<Property> primitives = _map.getPrimitives();
        Property p = null;
        if ( obj == null || ! keysAreEqual(obj, res) ) {
            obj = _map.newInstance();
            for (int i=0; i < _colId.length; i++) {
                p = primitives.get(i);
                p.setValue(obj, p.getValue(res,_colId[i]));
            }
        }
        if ( _lists != null && obj != null) {
            ArrayList<Property> lists = _map.getLists();
            ArrayList l = null;
            Object listObj = null;
            Object ret = null;
            for (int i=0; i < _lists.length; i++) {
                p = lists.get(i);
                l = (ArrayList)p.getValue(obj);
                if ( l == null) {
                    l = new ArrayList(); // Create a list
                    p.setValue(obj, l);
                    listObj = null;
                }
                else {
                    listObj = l.get(l.size()-1);
                }
                // Map list object
                if ( (ret = _lists[i].mapValues(listObj, res)) != listObj) {
                    l.add(ret);
                }
            }
        }
        return obj;
    }
}
