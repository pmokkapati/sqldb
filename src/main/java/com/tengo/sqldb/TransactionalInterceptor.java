/*
 * Interceptor for Transactional methods (Methods annotated with @Transactional)
 *
 * @author Prasad Mokkapati  prasadm80@gmail.com
 */
package com.tengo.sqldb;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;


public class TransactionalInterceptor implements MethodInterceptor {
    
    public Object invoke(MethodInvocation inv) throws Throwable {
        Object ret=null;
        boolean topLevel = true;

        if ( ! DBManager.beginTransaction() ) {
            topLevel = false;
        }
            
        try {
            ret = inv.proceed();
            if ( topLevel ) {
                DBManager.commitTransaction();
            }
                
        }
        catch (Exception e) {
            e.printStackTrace();
            rollback(inv, topLevel, e);
        }
        return ret;
    }

    private void rollback(MethodInvocation inv, boolean topLevel, Exception e) 
            throws DBException {
        DBManager.rollbackTransaction();
    }

}
