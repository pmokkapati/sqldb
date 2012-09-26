/*
 * Annotation to indicate how an enum needs to be mapped in the database
 *
 * @author Prasad Mokkapati  prasadm80@gmail.com
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
