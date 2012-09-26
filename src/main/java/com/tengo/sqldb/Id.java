/*
 * Annotation to indicate a particular property as an Unique Id (serial..)
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
 * Annotation to indicate a primary key - id - sequence/serial etc.
 */
public @interface Id {
}
