package edu.brown.hstore;

import org.voltdb.EELibraryLoader;
import org.voltdb.catalog.Site;

import edu.brown.BaseTestCase;
import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.ProjectType;

public class TestHStoreThreadManager extends BaseTestCase {

    HStoreConf hstore_conf = HStoreConf.singleton();
    HStoreThreadManager manager;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TM1);
        hstore_conf.site.cpu_affinity = true;
        
        Site catalog_site = CollectionUtil.first(CatalogUtil.getCluster(catalog).getSites());
        MockHStoreSite hstore_site = new MockHStoreSite(catalog_site, hstore_conf);
        this.manager = new HStoreThreadManager(hstore_site);
        EELibraryLoader.loadExecutionEngineLibrary(true);
    }
    
    /**
     * testRegisterProcessingThread
     */
    public void testRegisterProcessingThread() throws Exception {
        Thread self = Thread.currentThread();
        
        // Check whether we can register the thread and it doesn't
        // come back as disabled
        boolean ret = manager.registerProcessingThread();
        assertTrue(ret);
        assertTrue(manager.isEnabled());
        assertTrue(manager.isRegistered(self));
    }
    
}
