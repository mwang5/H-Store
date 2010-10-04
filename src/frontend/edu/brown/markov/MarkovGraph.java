package edu.brown.markov;

import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.voltdb.catalog.*;

import edu.brown.catalog.CatalogUtil;
import edu.brown.graphs.AbstractDirectedGraph;
import edu.brown.graphs.GraphvizExport;
import edu.brown.graphs.VertexTreeWalker;
import edu.brown.graphs.VertexTreeWalker.Direction;
import edu.brown.graphs.VertexTreeWalker.TraverseOrder;
import edu.brown.utils.AbstractTreeWalker;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.StringUtil;
import edu.brown.workload.*;

/**
 * Markov Model Graph
 * @author svelagap
 * @author pavlo
 */
public class MarkovGraph extends AbstractDirectedGraph<Vertex, Edge> implements Comparable<MarkovGraph> {
    private static final Logger LOG = Logger.getLogger(MarkovGraph.class);
    private static final long serialVersionUID = 3548405718926801012L;

    protected final Procedure catalog_proc;
    protected final int base_partition;

    /**
     * Cached references to the special marker vertices
     */
    private final transient HashMap<Vertex.Type, Vertex> vertex_cache = new HashMap<Vertex.Type, Vertex>();

    private int xact_count;

    // ----------------------------------------------------------------------------
    // CONSTRUCTORS
    // ----------------------------------------------------------------------------
    
    /**
     * Constructor
     * @param catalog_proc
     * @param basePartition
     */
    public MarkovGraph(Procedure catalog_proc, int basePartition,int xact_count) {
        super((Database) catalog_proc.getParent());
        this.catalog_proc = catalog_proc;
        this.base_partition = basePartition;
        this.xact_count = xact_count;
    }
    public MarkovGraph(Procedure catalog_proc, int basePartition){
        super((Database) catalog_proc.getParent());
        this.catalog_proc = catalog_proc;
        this.base_partition = basePartition;
    }
    /**
     * Add the START, COMMIT, and ABORT vertices to the current graph
     */
    public void initialize() {
        for (Vertex.Type type : Vertex.Type.values()) {
            switch (type) {
                case START:
                case COMMIT:
                case ABORT:
                    Vertex v = MarkovUtil.getSpecialVertex(this.getDatabase(), type);
                    assert(v != null);
                    this.addVertex(v);
                    break;
                default:
                    // IGNORE
            } // SWITCH
        }
    }
    
    @Override
    public String toString() {
        return (this.getClass().getSimpleName() + "<" + this.getProcedure().getName() + ", " + this.getBasePartition() + ">");
    }
    
    @Override
    public int compareTo(MarkovGraph o) {
        assert(o != null);
        return (this.catalog_proc.compareTo(o.catalog_proc));
    }
    
    // ----------------------------------------------------------------------------
    // DATA MEMBER METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Return the Procedure catalog object that this Markov graph represents
     * @return
     */
    public Procedure getProcedure() {
        return catalog_proc;
    }
    
    /**
     * Return the base partition id that this Markov graph represents
     * @return
     */
    public int getBasePartition() {
        return this.base_partition;
    }
    
    @Override
    public boolean addVertex(Vertex v) {
        boolean ret = super.addVertex(v);
        if (ret) {
            Vertex.Type type = v.getType();
            switch (type) {
                case START:
                case COMMIT:
                case ABORT:
                    assert(!this.vertex_cache.containsKey(type)) : "Trying add duplicate " + type + " vertex";
                    this.vertex_cache.put(type, v);
                    break;
                default:
                    // Ignore others
            } // SWITCH
        }
        return (ret);
    }
    
    /**
     * Get the vertex based on it's unique identifier. This is a combination of
     * the query, the partitions the query is touching and where the query is in
     * the transaction
     * 
     * @param a
     * @param partitions
     *            set of partitions this query is touching
     * @param queryInstanceIndex
     *            query's location in transactiontrace
     * @return
     */
    protected Vertex getVertex(Statement a, Set<Integer> partitions, long queryInstanceIndex) {
        for (Vertex v : this.getVertices()) {
            if (v.isEqual(a, partitions, queryInstanceIndex)) {
                return v;
            }
        }
        return null;
    }
    
    /**
     * Return an immutable list of all the partition ids in our catalog
     * @return
     */
    protected List<Integer> getAllPartitions() {
        return (CatalogUtil.getAllPartitions(this.getDatabase()));
    }
    
    /**
     * Gives all edges to the set of vertices vs in the MarkovGraph g
     * @param vs
     * @param g
     * @return
     */
    public Set<Edge> getEdgesTo(Set<Vertex> vs) {
        Set<Edge> edges = new HashSet<Edge>();
        for (Vertex v : vs) {
            if (v != null) edges.addAll(this.getInEdges(v));
        }
        return edges;
    }
    
    // ----------------------------------------------------------------------------
    // STATISTICAL MODEL METHODS
    // ----------------------------------------------------------------------------

    /**
     * Calculate the probabilities for this graph This invokes the static
     * methods in Vertex to calculate each probability
     */
    public synchronized void calculateProbabilities() {
        // Reset all probabilities
        for (Vertex v : this.getVertices()) {
            v.resetAllProbabilities();
        } // FOR
        
        // We first need to calculate the edge probabilities because the probabilities
        // at each vertex are going to be derived from these
        this.calculateEdgeProbabilities();
        
        // Then traverse the graph and calculate the vertex probability tables
        this.calculateVertexProbabilities();
    }

    /**
     * Calculate vertex probabilities
     */
    protected void calculateVertexProbabilities() {
        final boolean trace = (getProcedure().getName().equals("neworder") && base_partition == 1);
//        final boolean trace = LOG.isTraceEnabled(); 
        if (trace) {
            LOG.setLevel(Level.TRACE);
            LOG.trace("Calculating Vertex probabilities for " + this);
        }
        final Set<Edge> visited_edges = new HashSet<Edge>();
        final List<Integer> all_partitions = this.getAllPartitions();
        
        for (Vertex v : new Vertex[]{ this.getCommitVertex(), this.getAbortVertex() }) {
            new VertexTreeWalker<Vertex>(this, TraverseOrder.LONGEST_PATH, Direction.REVERSE) {
                @Override
                protected void callback(Vertex element) {
                    if (trace) LOG.trace("BEFORE: " + element + " => " + element.getSingleSitedProbability());
//                    if (element.isSingleSitedProbablitySet() == false) element.setSingleSitedProbability(0.0);
                    
                    // COMMIT/ABORT is always single-partitioned!
                    if (element.getType() == Vertex.Type.COMMIT || element.getType() == Vertex.Type.ABORT) {
                        if (trace) LOG.trace(element + " is single-partitioned!");
                        element.setSingleSitedProbability(1.0);
                        
                        // And DONE at all partitions!
                        // And will not Read/Write Probability
                        for (Integer partition : all_partitions) {
                            element.setDoneProbability(partition, 1.0);
                            element.setReadOnlyProbability(partition, 0.0);
                            element.setWriteProbability(partition, 0.0);
                        } // FOR
                        
                        // Abort Probability
                        if (element.getType() == Vertex.Type.ABORT) {
                            element.setAbortProbability(1.0);
                        } else {
                            element.setAbortProbability(0.0);
                        }
    
                    } else {
                        
                        // If the current vertex is not single-partitioned, then we know right away
                        // that the probability should be zero and we don't need to check our successors
                        // We define a single-partition vertex to be a query that accesses only one partition
                        // that is the same partition as the base/local partition. So even if the query accesses
                        // only one partition, if that partition is not the same as where the java is executing,
                        // then we're going to say that it is multi-partitioned
                        boolean element_islocalonly = element.isLocalPartitionOnly(getBasePartition()); 
                        if (element_islocalonly) {
                            if (trace) LOG.trace(element + " is NOT single-partitioned!");
                            element.setSingleSitedProbability(0.0);
                        }

                        Statement catalog_stmt = element.getCatalogItem();
                        
                        Collection<Edge> edges = MarkovGraph.this.getOutEdges(element);
                        for (Edge e : edges) {
                            if (visited_edges.contains(e)) continue;
                            Vertex successor = MarkovGraph.this.getDest(e);
                            assert(successor != null);
                            assert(successor.isSingleSitedProbabilitySet()) : "Setting " + element + " BEFORE " + successor;

                            // Single-Partition Probability
                            // We need to calculate the sum probability using the edge weights
                            if (element_islocalonly == false) {
                                double prob = (e.getProbability() * successor.getSingleSitedProbability());
                                if (trace) LOG.trace(element + " --" + e + "--> " + successor + String.format(" [%f * %f = %f]", e.getProbability(), successor.getSingleSitedProbability(), prob) + "\nprob = " + prob);
                                element.addSingleSitedProbability(prob);
                            }
                            
                            // Abort Probability
                            element.addAbortProbability(e.getProbability() * successor.getAbortProbability());
                            
                            // Done/Read/Write At Partition Probability
                            for (Integer partition : all_partitions) {
                                assert(successor.getDoneProbability(partition) != null) : "Setting " + element + " BEFORE " + successor;
                                assert(successor.getReadOnlyProbability(partition) != null) : "Setting " + element + " BEFORE " + successor;
                                assert(successor.getWriteProbability(partition) != null) : "Setting " + element + " BEFORE " + successor;
                                
                                // This vertex accesses this partition
                                if (element.getPartitions().contains(partition)) {
                                    element.setDoneProbability(partition, 0.0);
                                    
                                    // Figure out whether it is a read or a write
                                    if (catalog_stmt.getReadonly()) {
                                        element.addWriteProbability(partition, (e.getProbability() * successor.getWriteProbability(partition)));
                                        element.addReadOnlyProbability(partition, (e.getProbability() * successor.getReadOnlyProbability(partition)));
                                    } else {
                                        element.setWriteProbability(partition, 1.0);
                                        element.setReadOnlyProbability(partition, 0.0);
                                    }
                                    
                                // This vertex doesn't access the partition, but successor vertices might so
                                // the probability is based on the edge probabilities 
                                } else {
                                    element.addDoneProbability(partition, (e.getProbability() * successor.getDoneProbability(partition)));
                                    element.addWriteProbability(partition, (e.getProbability() * successor.getWriteProbability(partition)));
                                    element.addReadOnlyProbability(partition, (e.getProbability() * successor.getReadOnlyProbability(partition)));
                                }
                            } // FOR (PartitionId)
                        } // FOR (Edge)
                    }
                    if (trace) LOG.trace("AFTER: " + element + " => " + element.getSingleSitedProbability());
                    if (trace) LOG.trace(StringUtil.repeat("-", 40));
                }
            }.traverse(v);
        } // FOR (COMMIT, ABORT)
    }
    
    /**
     * Calculate the "done at a partition" probability for all vertices
     */
//    protected void calculateDoneProbability() {
//        final boolean trace = (getProcedure().getName().equals("neworder") && base_partition == 1); 
//        if (trace) LOG.debug("Calculating SINGLE-PARTITION probability");
//        
//         
//        final Set<Edge> visited_edges = new HashSet<Edge>();
//        
//        for (Vertex v : new Vertex[]{ this.getCommitVertex(), this.getAbortVertex() }) {
//            new VertexTreeWalker<Vertex>(this, TraverseOrder.LONGEST_PATH, Direction.REVERSE) {
//                private Set<Integer> partitions_seen = null;
//                private final Map<Vertex, Set<Integer>> vertex_partitions_seen = new HashMap<Vertex, Set<Integer>>();
//                
//                {
//                    if (trace) {
//    //                    VertexTreeWalker.LOG.setLevel(Level.TRACE);
//    //                    AbstractTreeWalker.LOG.setLevel(Level.TRACE);
//                    }
//                }
//                @Override
//                protected void callback_last(Vertex element) {
//    //                VertexTreeWalker.LOG.setLevel(Level.INFO);
//    //                AbstractTreeWalker.LOG.setLevel(Level.INFO);
//                }
//                
//                @Override
//                protected void callback_first(Vertex element) {
//                    this.partitions_seen = new HashSet<Integer>();
//                }
//                
//                @Override
//                protected void callback(Vertex element) {
//                    Set<Integer> current_partitions_seen = new HashSet<Integer>(this.partitions_seen);
//                    this.vertex_partitions_seen.put(element, current_partitions_seen);
//                    
//                    // COMMIT/ABORT is always finished at all partitions!
//                    if (element.getType() == Vertex.Type.COMMIT || element.getType() == Vertex.Type.ABORT) {
//                        
//                        
//                    // Otherwise, we have to look at the edges and figure out what partitions we're going touch
//                    } else {
//                        Collection<Edge> edges = MarkovGraph.this.getOutEdges(element);
//                        for (Edge e : edges) {
//                            if (visited_edges.contains(e)) continue;
//                            Vertex successor = MarkovGraph.this.getDest(e);
//                            assert(successor != null);
//                            
//                            // These are the partitions that we touch at this particular vertex
//                            Set<Integer> seensofar = new HashSet<Integer>(element.getPartitions());
//                            
//                            // These are the vertices that we haven't seen at all in the current path back
//                            // to the commit vertex. We will set their done probability to 1.0 for all
//                            // of these partitions if the probability is still null
//                            Set<Integer> current = new HashSet<Integer>(all_partitions);
//                            current.removeAll(this.partitions_seen);
//                            current.removeAll(seensofar);
//                            
//                            
//                            visited_edges.add(e);
//                        } // FOR
//                    }
//                    
//                    // Update the set of partitions that we've seen in the current path to include
//                    // the ones that we touched at this vertex.
//                    current_partitions_seen.addAll(element.getPartitions());
//                    this.partitions_seen = current_partitions_seen;
//                }
//                
//                @Override
//                protected void callback_after(Vertex element) {
//                    this.partitions_seen = this.vertex_partitions_seen.remove(element);
//                }
//            }.traverse(v);
//        } // FOR (COMMIT, ABORT)
//    }

    /**
     * Calculates the probabilities for each edge to be traversed
     */
    public void calculateEdgeProbabilities() {
        Collection<Vertex> vertices = this.getVertices();
        for (Vertex v : vertices) {
            for (Edge e : getOutEdges(v)) {
                e.setProbability(v.getTotalHits());
            }
        }
    }

    /**
     * Normalizes the times kept during online tallying of execution times.
     * TODO (svelagap): What about aborted transactions? Should they be counted in the normalization?
     */
    protected void normalizeTimes() {
        Map<Long, Long> stoptimes = this.getCommitVertex().getInstanceTimes();
        for (Vertex v : this.getVertices()) {
            v.normalizeInstanceTimes(stoptimes);
        }
    }
    
    /**
     * Checks to make sure the graph doesn't contain nonsense. We make sure
     * execution times and probabilities all add up correctly.
     * 
     * @return whether graph contains sane data
     */
    protected boolean isSane() {
        double EPSILON = 0.00001;
        for (Vertex v : getVertices()) {
            double sum = 0.0;
            Set<Vertex> seen_vertices = new HashSet<Vertex>(); 
            for (Edge e : this.getOutEdges(v)) {
                Vertex v1 = this.getOpposite(v, e);
                
                // Make sure that each vertex only has one edge to another vertex
                assert(!seen_vertices.contains(v1)) : "Vertex " + v + " has more than one edge to vertex " + v1;
                seen_vertices.add(v1);
                
                // Calculate total edge probabilities
                double edge_probability = e.getProbability(); 
                assert(edge_probability >= 0.0) : "Edge " + e + " probability is " + edge_probability;
                assert(edge_probability <= 1.0) : "Edge " + e + " probability is " + edge_probability;
                sum += e.getProbability();
            } // FOR
            
            if (sum - 1.0 > EPSILON && getInEdges(v).size() != 0) {
                return false;
            }
            sum = 0.0;
            // Andy asks: Should this be getInEdges()?
            // Saurya replies: No, the probability of leaving this query should be 1.0, coming
            //                 in could be more. There could be two vertices each with .75 probability
            //                 of getting into this vertex, 1.5 altogether
            for (Edge e : getOutEdges(v)) { 
                sum += e.getProbability();
            }
            if (sum - 1.0 > EPSILON && getOutEdges(v).size() != 0) {
                return false;
            }
        }
        return true;
    }

    public boolean shouldRecompute(int instance_count, double recomputeTolerance){
        double VERTEX_PROPORTION = 0.5f; //If VERTEX_PROPORTION of 
        int count = 0;
        for(Vertex v: this.getVertices()){
            if(v.shouldRecompute(instance_count, recomputeTolerance, xact_count)){
                count++;
            }
        }
        return (count*1.0/getVertices().size()) >= VERTEX_PROPORTION;
    }
    // ----------------------------------------------------------------------------
    // XACT PROCESSING METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Increases the weight between two vertices. Creates an edge if one does
     * not exist, then increments the source vertex's count and the edge's count
     * 
     * @param source
     *            - the source vertex
     * @param dest
     *            - the destination vertex
     */
    public void addToEdge(Vertex source, Vertex dest) {
        Edge e = this.findEdge(source, dest);
        if (e == null) {
            e = new Edge(this);
            this.addEdge(e, source, dest);
        }
        source.increment();
        e.increment();
    }
    
    /**
     * For a given TransactionTrace object, process its contents and update our
     * graph
     * 
     * @param xact_trace - The TransactionTrace to process and update the graph with
     * @param pest - The PartitionEstimator to use for estimating where things go
     */
    public List<Vertex> processTransaction(TransactionTrace xact_trace, PartitionEstimator pest) {
        Procedure catalog_proc = xact_trace.getCatalogItem(this.getDatabase());
        Vertex previous = this.getStartVertex();
        previous.addExecutionTime(xact_trace.getStopTimestamp() - xact_trace.getStartTimestamp());

        final List<Vertex> path = new ArrayList<Vertex>();
        path.add(previous);
        
        Map<Statement, AtomicInteger> query_instance_counters = new HashMap<Statement, AtomicInteger>();
        for (Statement catalog_stmt : catalog_proc.getStatements()) {
            query_instance_counters.put(catalog_stmt, new AtomicInteger(0));
        } // FOR
        
        // -----------QUERY TRACE-VERTEX CREATION--------------
        for (QueryTrace query_trace : xact_trace.getQueries()) {
            Set<Integer> partitions = null;
            try {
                partitions = pest.getPartitions(query_trace, base_partition);
            } catch (Exception e) {
                e.printStackTrace();
            }
            assert(partitions != null);
            assert(!partitions.isEmpty());
            Statement catalog_stmnt = query_trace.getCatalogItem(this.getDatabase());

            int queryInstanceIndex = query_instance_counters.get(catalog_stmnt).getAndIncrement(); 
            Vertex v = this.getVertex(catalog_stmnt, partitions, queryInstanceIndex);
            if (v == null) {
                // If no such vertex exists we simply create one
                v = new Vertex(catalog_stmnt, Vertex.Type.QUERY, queryInstanceIndex, partitions);
                this.addVertex(v);
            }
            // Add to the edge between the previous vertex and the current one
            this.addToEdge(previous, v);

            if (query_trace.getAborted()) {
                // Add an edge between the current vertex and the abort vertex
                this.addToEdge(v, this.getAbortVertex());
            }
            // Annotate the vertex with remaining execution time
            v.addExecutionTime(xact_trace.getStopTimestamp() - query_trace.getStartTimestamp());
            previous = v;
            path.add(v);
        } // FOR
        if (!previous.equals(this.getAbortVertex())) {
            this.addToEdge(previous, this.getCommitVertex());
            path.add(this.getCommitVertex());
        }
        // -----------END QUERY TRACE-VERTEX CREATION--------------
        this.xact_count++;
        return (path);
    }
    
    // ----------------------------------------------------------------------------
    // UTILITY METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Reset the instance hit counters
     * XXX: This assumes that this will not be manipulated concurrently, no
     * other transaction running at the same time
     */
    public synchronized void resetCounters() {
        for (Vertex v : this.getVertices()) {
            v.setInstancehits(0);
        }
        for (Edge e : this.getEdges()) {
            e.setInstancehits(0);
        }
    }
    
    /**
     * Update the instance hits for the graph's elements and recalculate probabilities
     */
    public synchronized void recomputeGraph() {
        this.normalizeTimes();
        for (Vertex v : this.getVertices()) {
            v.incrementTotalhits(v.getInstancehits());
        }
        for (Edge e : this.getEdges()) {
            e.incrementHits(e.getInstancehits());
        }
        this.calculateProbabilities();
    }
    
    /**
     * 
     * @return The number of xacts used to make this MarkovGraph
     */
    public int getTransactionCount() {
        return xact_count;
    }    
    public void setTransactionCount(int xact_count){
        this.xact_count = xact_count;
    }
    /**
     * Increase the transaction count for this MarkovGraph by one
     */
    public void incrementTransasctionCount() {
       this.xact_count++; 
    }
    
    /**
     * For the given Vertex type, return the special vertex
     * @param type - the Vertex type (cannot be a regular query)
     * @return the single vertex for that type  
     */
    protected final Vertex getVertex(Vertex.Type type) {
        Vertex ret = this.vertex_cache.get(type);
        assert(ret != null) : "The special vertex for type " + type + " is null";
        return (ret);
    }
    
    /**
     * Get the start vertex for this MarkovGraph
     * @return
     */
    public final Vertex getStartVertex() {
        return (this.getVertex(Vertex.Type.START));
    }
    /**
     * Get the stop vertex for this MarkovGraph
     * @return
     */
    public final Vertex getCommitVertex() {
        return (this.getVertex(Vertex.Type.COMMIT));
    }
    /**
     * Get the abort vertex for this MarkovGraph
     * @return
     */
    public final Vertex getAbortVertex() {
        return (this.getVertex(Vertex.Type.ABORT));
    }

    // ----------------------------------------------------------------------------
    // YE OLDE MAIN METHOD
    // ----------------------------------------------------------------------------

    /**
     * To load in the workloads and see their properties, we use this method.
     * There are still many workloads that have problems running.
     * 
     * @param args
     * @throws InterruptedException
     * @throws InvocationTargetException
     */
    public static void main(String vargs[]) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        args.require(ArgumentsParser.PARAM_CATALOG, ArgumentsParser.PARAM_WORKLOAD);
        final PartitionEstimator p_estimator = new PartitionEstimator(args.catalog_db, args.hasher);
        MarkovGraphsContainer graphs_per_partition = MarkovUtil.createGraphs(args.catalog_db, args.workload, p_estimator);
        
//
//        Map<Procedure, Pair<Integer, Integer>> counts = new HashMap<Procedure, Pair<Integer, Integer>>();
//        for (Procedure catalog_proc : args.catalog_db.getProcedures()) {
//            int vertexcount = 0;
//            int edgecount = 0;
//            
//            for (int i : partitions) {
//                Pair<Procedure, Integer> current = new Pair<Procedure, Integer>(catalog_proc, i);
//                vertexcount += partitionGraphs.get(current).getVertexCount();
//                edgecount += partitionGraphs.get(current).getEdgeCount();
//            } // FOR
//            counts.put(catalog_proc, new Pair<Integer, Integer>(vertexcount, edgecount));
//        } // FOR
//        for (Procedure pr : counts.keySet()) {
//            System.out.println(pr + "," + args.workload + "," + args.workload_xact_limit + ","
//                    + counts.get(pr).getFirst() + ","
//                    + counts.get(pr).getSecond());
//        } // FOR
        
        //
        // Save the graphs
        //
        if (args.hasParam(ArgumentsParser.PARAM_MARKOV_OUTPUT)) {
            LOG.info("Writing graphs out to " + args.getParam(ArgumentsParser.PARAM_MARKOV_OUTPUT));
            MarkovUtil.save(graphs_per_partition, args.getParam(ArgumentsParser.PARAM_MARKOV_OUTPUT));
//            for (Integer partition : graphs_per_partition.keySet()) {
//                for (MarkovGraph g : graphs_per_partition.get(partition)) {
//                    String name = g.getProcedure() + "_" + partition;
//                    String contents = GraphvizExport.export(g, name);
//                    FileUtil.writeStringToFile("./graphs/" + name + ".dot", contents);
//                }
//            }
            
        }
    }
}