/*
 * Annotation to indicate that a function/method is transactional
 *
 * @author Prasad Mokkapati  prasadm80@gmail.com
 */
package com.tengo.sqldb;

import java.lang.annotation.Documented;
import java.lang.annotation.Inherited;
import java.lang.annotation.Target;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Inherited
@Documented
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)

public @interface Transactional {
}
