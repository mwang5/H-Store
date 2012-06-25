package edu.brown.hstore.dispatchers;

import java.nio.ByteBuffer;

import org.apache.log4j.Logger;
import org.voltdb.utils.Pair;

import edu.brown.hstore.HStoreCoordinator;
import edu.brown.hstore.callbacks.TransactionRedirectResponseCallback;

/**
 * 
 */
public class TransactionRedirectDispatcher extends AbstractDispatcher<Pair<ByteBuffer, TransactionRedirectResponseCallback>> {
    private static final Logger LOG = Logger.getLogger(TransactionRedirectDispatcher.class);
    
    public TransactionRedirectDispatcher(HStoreCoordinator hStoreCoordinator) {
        super(hStoreCoordinator);
    }

    @Override
    public void runImpl(Pair<ByteBuffer, TransactionRedirectResponseCallback> p) {
        this.hstore_coordinator.getHStoreSite().queueInvocation(p.getFirst(), p.getSecond());
    }
}