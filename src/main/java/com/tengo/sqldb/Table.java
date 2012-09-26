/*
 * Annotation to specify database table to map to
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
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)

public @interface Table {
    String name() default "";
    String[] uniqueConstraints() default {};
}
