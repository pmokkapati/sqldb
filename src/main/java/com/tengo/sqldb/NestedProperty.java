/*
 * Property that is nested .. I.E. Returns another bean
 *
 * @author Prasad Mokkapati  prasadm80@gmail.com
 */
package com.tengo.sqldb;

import java.util.ArrayList;
/**
 * Class to store a nested property. I.E. a property of a bean ..
 */
public class NestedProperty extends Property {
    private ArrayList<Property> _list;


    public  void addProperty(Property p) {
        if ( _list == null) {
            _list = new ArrayList<Property>();
        }
        if ( p instanceof NestedProperty) {
            _list.addAll( ((NestedProperty)p)._list);
        }
        else {
            _list.add(p);
        }
    }
    public NestedProperty(Property p, Property next) {
        super(PropertyType.Nested);
        addProperty(p);
        addProperty(next);
    }

    public NestedProperty(NestedProperty p) {
        super(PropertyType.Nested);
        addProperty(p);
    }

        
    /**
     * Function to return the value of this property 
     * @param o Object from which to get the value from
     * @return returns the value object
     */
    public Object getValue(Object o) throws DBException {
        Object ret = o;
        for (Property p: _list) {
            if ( (ret = p.getValue(ret)) == null) {
                return null;
            }
        }
        return ret;
    }
    /**
     * Function to set the value for this property in the given object
     * @param o Object/bean into which the value needs to be set
     * @param v value for this property
     */
    public void setValue(Object o, Object v) throws DBException {
        Object ret = o;
        Property p = null;
        int cnt = _list.size();
        for (int i=0; i < cnt -1; i++) {
            p = _list.get(i);
            if ( (ret = p.getValue(o)) == null) {
                ret = p.newInstance();
                p.setValue(o, ret);
            }
            o = ret;
        }
        p.setValue(o, v);
    }
}
