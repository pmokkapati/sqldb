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
 * Annotation on a bean property which returns enum type
 */
public @interface Enumerated {
    public enum EnumType { ORDINAL, STRING};
    EnumType value() default EnumType.ORDINAL;
}
