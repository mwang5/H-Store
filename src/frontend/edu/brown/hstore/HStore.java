/***************************************************************************
 *   Copyright (C) 2011 by H-Store Project                                 *
 *   Brown University                                                      *
 *   Massachusetts Institute of Technology                                 *
 *   Yale University                                                       *
 *                                                                         *
 *   Permission is hereby granted, free of charge, to any person obtaining *
 *   a copy of this software and associated documentation files (the       *
 *   "Software"), to deal in the Software without restriction, including   *
 *   without limitation the rights to use, copy, modify, merge, publish,   *
 *   distribute, sublicense, and/or sell copies of the Software, and to    *
 *   permit persons to whom the Software is furnished to do so, subject to *
 *   the following conditions:                                             *
 *                                                                         *
 *   The above copyright notice and this permission notice shall be        *
 *   included in all copies or substantial portions of the Software.       *
 *                                                                         *
 *   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,       *
 *   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF    *
 *   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.*
 *   IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR     *
 *   OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, *
 *   ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR *
 *   OTHER DEALINGS IN THE SOFTWARE.                                       *
 ***************************************************************************/
package edu.brown.hstore;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.apache.log4j.Logger;
import org.voltdb.BackendTarget;
import org.voltdb.ProcedureProfiler;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Site;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.mappings.ParameterMappingsSet;
import edu.brown.markov.MarkovUtil;
import edu.brown.markov.TransactionEstimator;
import edu.brown.markov.containers.MarkovGraphContainersUtil;
import edu.brown.markov.containers.MarkovGraphsContainer;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.PartitionEstimator;
import edu.brown.workload.Workload;

/**
 * Main Wrapper Class for an HStoreSite
 * @author pavlo
 */
public abstract class HStore {
    public static final Logger LOG = Logger.getLogger(HStore.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.setupLogging();
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    private static HStoreSite singleton;
    private static String buildString;
    private static String versionString;
    
    
    /**
     * Initialize the HStore server.
     */
    public synchronized static final HStoreSite initialize(Site catalog_site, HStoreConf hstore_conf) {
        singleton = new HStoreSite(catalog_site, hstore_conf);
        
        // For every partition in our local site, we want to setup a new ExecutionSite
        // Thankfully I had enough sense to have PartitionEstimator take in the local partition
        // as a parameter, so we can share a single instance across all ExecutionSites
        PartitionEstimator p_estimator = singleton.getPartitionEstimator();
        
        // ----------------------------------------------------------------------------
        // MarkovGraphs
        // ----------------------------------------------------------------------------
        Map<Integer, MarkovGraphsContainer> markovs = null;
        if (hstore_conf.site.markov_path != null) {
            File path = new File(hstore_conf.site.markov_path);
            if (path.exists()) {
                Database catalog_db = CatalogUtil.getDatabase(catalog_site);
                try {
                    markovs = MarkovGraphContainersUtil.loadIds(catalog_db,
                                                                path.getAbsolutePath(), 
                                                                CatalogUtil.getLocalPartitionIds(catalog_site));
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
                MarkovGraphContainersUtil.setHasher(markovs, singleton.getPartitionEstimator().getHasher());
                LOG.info("Finished loading MarkovGraphsContainer '" + path + "'");
            } else if (debug.get()) LOG.warn("The Markov Graphs file '" + path + "' does not exist");
        }
        
        // ----------------------------------------------------------------------------
        // ParameterMappings
        // ----------------------------------------------------------------------------
        ParameterMappingsSet mappings = new ParameterMappingsSet(); 
        if (hstore_conf.site.mappings_path != null) {
            File path = new File(hstore_conf.site.mappings_path);
            if (path.exists()) {
                Database catalog_db = CatalogUtil.getDatabase(catalog_site);
                try {
                    mappings.load(path.getAbsolutePath(), catalog_db);
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            } else if (debug.get()) LOG.warn("The ParameterMappings file '" + path + "' does not exist");
        }
        
        // ----------------------------------------------------------------------------
        // PartitionExecutor Initialization
        // ----------------------------------------------------------------------------
        for (Partition catalog_part : catalog_site.getPartitions()) {
            int local_partition = catalog_part.getId();
            MarkovGraphsContainer local_markovs = null;
            if (markovs != null) {
                if (markovs.containsKey(MarkovUtil.GLOBAL_MARKOV_CONTAINER_ID)) {
                    local_markovs = markovs.get(MarkovUtil.GLOBAL_MARKOV_CONTAINER_ID);
                } else {
                    local_markovs = markovs.get(local_partition);
                }
                assert(local_markovs != null) : "Failed to get the proper MarkovGraphsContainer that we need for partition #" + local_partition;
            }

            // Initialize TransactionEstimator stuff
            // Load the Markov models if we were given an input path and pass them to t_estimator
            // Load in all the partition-specific TransactionEstimators and ExecutionSites in order to 
            // stick them into the HStoreSite
            if (debug.get()) LOG.debug("Creating Estimator for " + HStoreThreadManager.formatSiteName(catalog_site.getId()));
            TransactionEstimator t_estimator = new TransactionEstimator(p_estimator, mappings, local_markovs);

            // setup the EE
            if (debug.get()) LOG.debug("Creating ExecutionSite for Partition #" + local_partition);
            PartitionExecutor executor = new PartitionExecutor(
                    local_partition,
                    catalog_site.getCatalog(),
                    BackendTarget.NATIVE_EE_JNI, // BackendTarget.NULL,
                    p_estimator,
                    t_estimator);
            singleton.addPartitionExecutor(local_partition, executor);
        } // FOR
        
        return (singleton);
    }
    
    /**
     * Retrieve a reference to the main HStoreSite running in this JVM. 
     * When running a real server (and not a test harness), this instance will only
     * be useful after calling HStore.initialize().
     *
     * @return A reference to the underlying HStoreSite object.
     */
    public static final HStoreSite instance() {
        return singleton;
    }
    
    /**
     * Main Start-up Method
     * @param vargs
     * @throws Exception
     */
    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs,
                    ArgumentsParser.PARAM_CATALOG,
                    ArgumentsParser.PARAM_SITE_ID,
                    ArgumentsParser.PARAM_CONF
        );
        
        // HStoreSite Stuff
        final int site_id = args.getIntParam(ArgumentsParser.PARAM_SITE_ID);
        Thread t = Thread.currentThread();
        t.setName(HStoreThreadManager.getThreadName(site_id, null, "main"));
        
        final Site catalog_site = CatalogUtil.getSiteFromId(args.catalog_db, site_id);
        if (catalog_site == null) throw new RuntimeException("Invalid site #" + site_id);
        
        HStoreConf hstore_conf = HStoreConf.initArgumentsParser(args, catalog_site);
        if (debug.get()) 
            LOG.debug("HStoreConf Parameters:\n" + HStoreConf.singleton().toString(true));
        
        HStoreSite hstore_site = HStore.initialize(catalog_site, hstore_conf);

        // ----------------------------------------------------------------------------
        // Workload Trace Output
        // ----------------------------------------------------------------------------
        if (args.hasParam(ArgumentsParser.PARAM_WORKLOAD_OUTPUT)) {
            ProcedureProfiler.profilingLevel = ProcedureProfiler.Level.INTRUSIVE;
            String traceClass = Workload.class.getName();
            String tracePath = args.getParam(ArgumentsParser.PARAM_WORKLOAD_OUTPUT) + "-" + site_id;
            String traceIgnore = args.getParam(ArgumentsParser.PARAM_WORKLOAD_PROC_EXCLUDE);
            ProcedureProfiler.initializeWorkloadTrace(args.catalog, traceClass, tracePath, traceIgnore);
            LOG.info("Enabled workload logging '" + tracePath + "'");
        }
        
        if (args.thresholds != null) hstore_site.setThresholds(args.thresholds);
        
        // ----------------------------------------------------------------------------
        // Bombs Away!
        // ----------------------------------------------------------------------------
        LOG.info("Instantiating HStoreSite network connections...");
        hstore_site.run();
    }
    
    public static String getBuildString() {
        if (buildString == null) {
            synchronized (HStore.class) {
                if (buildString == null) readBuildInfo();
            } // SYNCH
        }
        return (buildString);
    }
    
    public static String getVersionString() {
        if (versionString == null) {
            synchronized (HStore.class) {
                if (versionString == null) readBuildInfo();
            } // SYNCH
        }
        return (versionString);
    }
    
    private static void readBuildInfo() {
        StringBuilder sb = new StringBuilder(64);
        byte b = -1;
        try {
            InputStream buildstringStream =
                ClassLoader.getSystemResourceAsStream("buildstring.txt");
            while ((b = (byte) buildstringStream.read()) != -1) {
                sb.append((char)b);
            }
            sb.append("\n");
            String parts[] = sb.toString().split(" ", 2);
            if (parts.length != 2) {
                throw new RuntimeException("Invalid buildstring.txt file.");
            }
            versionString = parts[0].trim();
            buildString = parts[1].trim();
        } catch (Exception ignored) {
            try {
                InputStream buildstringStream = new FileInputStream("version.txt");
                while ((b = (byte) buildstringStream.read()) != -1) {
                    sb.append((char)b);
                }
                versionString = sb.toString().trim();
            }
            catch (Exception ignored2) {
                throw new RuntimeException(ignored);
            }
        } finally {
            if (buildString == null) buildString = "H-Store";
        }
        LOG.info("Build: " + versionString + " " + buildString);
    }
}
