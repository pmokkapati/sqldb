/*
 * Copyright 2012 by Tengo, Inc.
 * All rights reserved.
 *
 * This software is the confidential and proprietary information 
 * of Tengo, Inc.
 *
 * @author psm
 */
package com.tengo.sqldb.inject.module;

import com.google.inject.AbstractModule;
import com.google.inject.matcher.Matchers;

import com.tengo.sqldb.TransactionalInterceptor;
import com.tengo.sqldb.Transactional;

public class TransactionModule extends AbstractModule {
    
    @Override
    protected void configure() {
        TransactionalInterceptor i = new TransactionalInterceptor();
        bindInterceptor(Matchers.any(), 
            Matchers.annotatedWith(Transactional.class), i);
    }
}
