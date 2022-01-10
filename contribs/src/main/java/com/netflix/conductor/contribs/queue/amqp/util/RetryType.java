/*
 * $Id$
 * 
 * Copyright (c) 2020 NetApp, Inc.
 * All rights reserved.
 */
package com.netflix.conductor.contribs.queue.amqp.util;

/**
 * RetryType holds the retry type constants to wait for either same period to retry or for exponential time increase
 *
 */
public enum RetryType {

   REGULARINTERVALS, EXPONENTIALBACKOFF, INCREMENTALINTERVALS
}