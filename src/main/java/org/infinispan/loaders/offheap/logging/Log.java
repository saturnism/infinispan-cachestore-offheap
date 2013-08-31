package org.infinispan.loaders.offheap.logging;

import org.jboss.logging.MessageLogger;

/**
 * Log abstraction for the Offheap cache store. For this module, message ids ranging from 24001 to
 * 25000 inclusively have been reserved.
 * 
 * @author Ray Tsang
 * @since 6.0
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends org.infinispan.util.logging.Log {
}
