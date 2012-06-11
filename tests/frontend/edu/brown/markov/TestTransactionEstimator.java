package edu.brown.markov;

import java.io.File;
import java.util.*;

import org.junit.Test;
import org.voltdb.VoltProcedure;
import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.catalog.*;
import org.voltdb.types.ExpressionType;

import edu.brown.BaseTestCase;
import edu.brown.catalog.CatalogUtil;
import edu.brown.mappings.ParameterMappingsSet;
import edu.brown.markov.TransactionEstimator.State;
import edu.brown.markov.containers.MarkovGraphContainersUtil;
import edu.brown.markov.containers.MarkovGraphsContainer;
import edu.brown.utils.*;
import edu.brown.workload.QueryTrace;
import edu.brown.workload.Workload;
import edu.brown.workload.TransactionTrace;
import edu.brown.workload.filters.BasePartitionTxnFilter;
import edu.brown.workload.filters.Filter;
import edu.brown.workload.filters.NoAbortFilter;
import edu.brown.workload.filters.ProcParameterArraySizeFilter;
import edu.brown.workload.filters.ProcParameterValueFilter;
import edu.brown.workload.filters.ProcedureLimitFilter;
import edu.brown.workload.filters.ProcedureNameFilter;

/**
 * 
 * @author pavlo
 *
 */
public class TestTransactionEstimator extends BaseTestCase {

    public static final Random rand = new Random();
    
    private static final int WORKLOAD_XACT_LIMIT = 50;
    private static final int BASE_PARTITION = 1;
    private static final int NUM_PARTITIONS = 10;
    private static Collection<Integer> ALL_PARTITIONS;
    private static final Class<? extends VoltProcedure> TARGET_PROCEDURE = neworder.class;
    private static int XACT_ID = 1000;

    private static Workload workload;
    private static MarkovGraphsContainer markovs;
    private static ParameterMappingsSet correlations;
    private static TransactionTrace singlep_trace;
    private static TransactionTrace multip_trace;
    private static final Set<Integer> multip_partitions = new HashSet<Integer>();
    private static final List<MarkovVertex> multip_path = new ArrayList<MarkovVertex>();
    
    private TransactionEstimator t_estimator;
    private EstimationThresholds thresholds;
    private Procedure catalog_proc;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);
        this.addPartitions(NUM_PARTITIONS);
        ALL_PARTITIONS = CatalogUtil.getAllPartitionIds(catalog_db);
        
        this.catalog_proc = this.getProcedure(TARGET_PROCEDURE);
        
        if (markovs == null) {
            File file = this.getParameterMappingsFile(ProjectType.TPCC);
            correlations = new ParameterMappingsSet();
            correlations.load(file.getAbsolutePath(), catalog_db);
            
            Filter filter = new ProcedureNameFilter(false)
                    .include(TARGET_PROCEDURE.getSimpleName())
                    .attach(new NoAbortFilter())
                    .attach(new ProcParameterValueFilter().include(1, new Integer(5))) // D_ID
                    .attach(new ProcParameterArraySizeFilter(CatalogUtil.getArrayProcParameters(catalog_proc).get(0), 10, ExpressionType.COMPARE_EQUAL))
                    .attach(new BasePartitionTxnFilter(p_estimator, BASE_PARTITION))
                    .attach(new ProcedureLimitFilter(WORKLOAD_XACT_LIMIT));

            file = this.getWorkloadFile(ProjectType.TPCC);
            workload = new Workload(catalog);
            ((Workload) workload).load(file.getAbsolutePath(), catalog_db, filter);
            assert(workload.getTransactionCount() > 0);
            
            // Generate MarkovGraphs
            markovs = MarkovGraphContainersUtil.createBasePartitionMarkovGraphsContainer(catalog_db, workload, p_estimator);
            assertNotNull(markovs);
            
            // Find a single-partition and multi-partition trace
            multip_partitions.add(BASE_PARTITION);
            for (TransactionTrace xact : workload.getTransactions()) {
                Object ol_supply_w_ids[] = (Object[])xact.getParam(5);
                assert(ol_supply_w_ids != null);
                boolean same_partition = true;
                for (Object i : ol_supply_w_ids) {
                    Integer partition = p_estimator.getHasher().hash(Integer.valueOf(i.toString()));
                    same_partition = same_partition && (partition == BASE_PARTITION);
                    if (same_partition == false && multip_trace == null) {
                        multip_partitions.add(partition);
                    }
                } // FOR
                if (same_partition && singlep_trace == null) singlep_trace = xact;
                if (same_partition == false && multip_trace == null) {
                    multip_trace = xact;
                    multip_path.addAll(markovs.get(BASE_PARTITION, this.catalog_proc).processTransaction(xact, p_estimator));
                }
                if (singlep_trace != null && multip_trace != null) break;
            } // FOR
        }
        assertNotNull(multip_trace);
        assert(multip_partitions.size() > 1);
        assertFalse(multip_path.isEmpty());
//        assertNotNull(singlep_trace);
        
        // Setup
        this.t_estimator = new TransactionEstimator(p_estimator, correlations, markovs);
        this.thresholds = new EstimationThresholds();
    }

    /**
     * testMultipleStartTransaction
     */
    @Test
    public void testMultipleStartTransaction() throws Exception {
        Set<State> all_states = new HashSet<State>();
        
        for (int i = 0; i < 20; i++) {
            State state = t_estimator.startTransaction(XACT_ID++, this.catalog_proc, multip_trace.getParams());
            assertNotNull(state);
            assertFalse(all_states.contains(state));
            all_states.add(state);
        } // FOR
    }
    
    /**
     * testStartTransaction
     */
    @Test
    public void testStartTransaction() throws Exception {
        long txn_id = XACT_ID++;
        State state = t_estimator.startTransaction(txn_id, this.catalog_proc, singlep_trace.getParams());
        assertNotNull(state);
        MarkovEstimate est = state.getInitialEstimate();
        assertNotNull(est);
        assertNotNull(state.getLastEstimate());
//        System.err.println(est.toString());
        
        MarkovGraph markov = markovs.get(BASE_PARTITION, this.catalog_proc);
        List<MarkovVertex> initial_path = state.getInitialPath();
        assertFalse(initial_path.isEmpty());
        
        System.err.println("# of Vertices: " + markov.getVertexCount());
        System.err.println("# of Edges:    " + markov.getEdgeCount());
        System.err.println("Confidence:    " + String.format("%.4f", t_estimator.getConfidence(txn_id)));
        System.err.println("\nINITIAL PATH:\n" + StringUtil.join("\n", initial_path));
//        System.err.println(multip_trace.debug(catalog_db));

        Set<Integer> partitions = p_estimator.getAllPartitions(singlep_trace);
        assertNotNull(partitions);
//        assert(partitions.size() > 1) : partitions;
        System.err.println("partitions: " + partitions);
        
//        GraphvizExport<Vertex, Edge> gv = MarkovUtil.exportGraphviz(markov, false, null);
//        gv.highlightPath(markov.getPath(initial_path), "blue");
//        gv.writeToTempFile(this.catalog_proc, 0);
//
//        MarkovUtil.exportGraphviz(markov, false, markov.getPath(multip_path)).writeToTempFile(this.catalog_proc, 1);
        
        Set<Integer> est_partitions = est.getTouchedPartitions(thresholds);
        assertNotNull(est_partitions);
        assertEquals(partitions.size(), est_partitions.size());
        assertEquals(partitions, est_partitions);
        
        assert(est.isSinglePartition(this.thresholds));
        assertTrue(est.isAbortable(this.thresholds));
        
        for (Integer partition : ALL_PARTITIONS) {
            if (partitions.contains(partition)) { //  == BASE_PARTITION) {
                assertFalse("isFinishedPartition(" + partition + ")", est.isFinishedPartition(thresholds, partition));
                assertTrue("isWritePartition(" + partition + ")", est.isWritePartition(thresholds, partition) == true);
                assertTrue("isTargetPartition(" + partition + ")", est.isTargetPartition(thresholds, partition) == true);
            } else {
                assertTrue("isFinishedPartition(" + partition + ")", est.isFinishedPartition(thresholds, partition));
                assertFalse("isWritePartition(" + partition + ")", est.isWritePartition(thresholds, partition) == true);
                assertFalse("isTargetPartition(" + partition + ")", est.isTargetPartition(thresholds, partition) == true);
            }
        } // FOR
    }
    
    /**
     * testProcessTransactionTrace
     */
    @Test
    public void testProcessTransactionTrace() throws Exception {
        TransactionTrace txn_trace = CollectionUtil.first(workload.getTransactions());
        assertNotNull(txn_trace);
        State s = this.t_estimator.processTransactionTrace(txn_trace);
        assertNotNull(s);
        
        // We should have an MarkovEstimate for each batch
        assertEquals(txn_trace.getBatchCount(), s.getEstimateCount());
        List<MarkovEstimate> estimates = s.getEstimates();
        for (int i = 0, cnt = txn_trace.getBatchCount(); i < cnt; i++) {
            List<QueryTrace> queries = txn_trace.getBatchQueries(i);
            assertFalse(queries.isEmpty());
            
            MarkovEstimate est = estimates.get(i);
            assertNotNull(est);
            
            // The last vertex in each MarkovEstimate should correspond to the last query in each batch
            MarkovVertex last_v = est.getVertex();
            assertNotNull(last_v);
            assertEquals(CollectionUtil.last(queries).getCatalogItem(catalog_db), last_v.getCatalogItem());
        } // FOR
    }
}
