package org.voltdb.regressionsuites;

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;

import edu.brown.benchmark.voter.VoterConstants;
import edu.brown.benchmark.voter.VoterProjectBuilder;
import edu.brown.benchmark.voter.procedures.Initialize;
import edu.brown.benchmark.voter.procedures.Vote;
import edu.brown.hstore.Hstoreservice.Status;

/**
 * Simple test suite for the VOTER benchmark
 * @author pavlo
 */
public class TestVoterSuite extends RegressionSuite {

    private static final long phoneNumber = 8675309; // Jenny
    private static final int contestantNumber = 1;
    private static final long maxVotesPerPhoneNumber = 5;
    
    /**
     * Constructor needed for JUnit. Should just pass on parameters to superclass.
     * @param name The name of the method to test. This is just passed to the superclass.
     */
    public TestVoterSuite(String name) {
        super(name);
    }
    
    private void initializeDatabase(Client client) throws Exception {
        Object params[] = {
            VoterConstants.NUM_CONTESTANTS,
            VoterConstants.CONTESTANT_NAMES_CSV
        };
        
        ClientResponse cresponse = client.callProcedure(Initialize.class.getSimpleName(), params);
        assertNotNull(cresponse);
        assertEquals(Status.OK, cresponse.getStatus());
    }

    /**
     * testInitialize
     */
    public void testInitialize() throws Exception {
        Client client = this.getClient();
        this.initializeDatabase(client);
        
        String query = "SELECT COUNT(*) FROM contestants";
        ClientResponse cresponse = client.callProcedure("@AdHoc", query);
        assertEquals(Status.OK, cresponse.getStatus());
        VoltTable results[] = cresponse.getResults();
        assertEquals(1, results.length);
        assertEquals(VoterConstants.NUM_CONTESTANTS, results[0].asScalarLong());
        System.err.println(results[0]);
    }

    /**
     * testVote
     */
    public void testVote() throws Exception {
        Client client = this.getClient();
        this.initializeDatabase(client);
        
        ClientResponse cresponse = client.callProcedure(Vote.class.getSimpleName(),
                                                        phoneNumber,
                                                        contestantNumber,
                                                        maxVotesPerPhoneNumber);
        assertEquals(Status.OK, cresponse.getStatus());
        VoltTable results[] = cresponse.getResults();
        assertEquals(1, results.length);
        assertEquals(Vote.VOTE_SUCCESSFUL, results[0].asScalarLong());
        
        // Make sure that our vote is actually in the real table and materialized views
        String query = "SELECT COUNT(*) FROM votes";
        cresponse = client.callProcedure("@AdHoc", query);
        assertEquals(Status.OK, cresponse.getStatus());
        results = cresponse.getResults();
        assertEquals(1, results.length);
        assertEquals(1, results[0].asScalarLong());
        
        query = "SELECT * FROM v_votes_by_phone_number";
        cresponse = client.callProcedure("@AdHoc", query);
        assertEquals(Status.OK, cresponse.getStatus());
        results = cresponse.getResults();
        assertEquals(1, results.length);
        System.err.println(results[0]);
        assertTrue(results[0].advanceRow());
        assertEquals(phoneNumber, results[0].getLong(0));
        assertEquals(1, results[0].getLong(1));
    }
    
    /**
     * testVoteLimit
     */
    public void testVoteLimit() throws Exception {
        Client client = this.getClient();
        this.initializeDatabase(client);
        
        // Make sure that the phone number is only allowed to vote up to
        // the limit and not anymore after that
        ClientResponse cresponse = null;
        for (int i = 0, cnt = (int)(maxVotesPerPhoneNumber*2); i < cnt; i++) {
            long expected = (i < maxVotesPerPhoneNumber ? Vote.VOTE_SUCCESSFUL :
                                                          Vote.ERR_VOTER_OVER_VOTE_LIMIT);
            cresponse = client.callProcedure(Vote.class.getSimpleName(),
                                             phoneNumber,
                                             contestantNumber,
                                             maxVotesPerPhoneNumber);
            assertEquals(Status.OK, cresponse.getStatus());
            VoltTable results[] = cresponse.getResults();
            assertEquals(1, results.length);
            assertEquals(expected, results[0].asScalarLong());
        } // FOR
    }
        

    public static Test suite() {
        VoltServerConfig config = null;
        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestVoterSuite.class);

        // build up a project builder for the TPC-C app
        VoterProjectBuilder project = new VoterProjectBuilder();
        project.addAllDefaults();
        
        boolean success;
        
        /////////////////////////////////////////////////////////////
        // CONFIG #1: 1 Local Site/Partition running on JNI backend
        /////////////////////////////////////////////////////////////
        config = new LocalSingleProcessServer("voter-1part.jar", 1, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);
        
        /////////////////////////////////////////////////////////////
        // CONFIG #2: 1 Local Site with 2 Partitions running on JNI backend
        /////////////////////////////////////////////////////////////
        config = new LocalSingleProcessServer("voter-2part.jar", 2, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        ////////////////////////////////////////////////////////////
        // CONFIG #3: cluster of 2 nodes running 2 site each, one replica
        ////////////////////////////////////////////////////////////
        config = new LocalCluster("voter-cluster.jar", 2, 2, 1, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        return builder;
    }

}
