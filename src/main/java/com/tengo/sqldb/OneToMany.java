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

import java.lang.annotation.Documented;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.lang.annotation.ElementType;
import java.lang.annotation.RetentionPolicy;

@Inherited
@Documented
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)

/**
 * Annotation on a bean property - getter/setter method to indicate mapping
 * for one to many (linked tables).
 */
public @interface OneToMany {
    String[] mappedBy();
}
