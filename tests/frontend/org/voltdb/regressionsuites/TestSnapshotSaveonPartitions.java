package org.voltdb.regressionsuites;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;

import org.junit.Test;
import org.voltdb.DefaultSnapshotDataTarget;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.Procedure;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.utils.SnapshotVerifier;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.tm1.procedures.DeleteCallForwarding;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.utils.ProjectType;

public class TestSnapshotSaveonPartitions extends RegressionSuite{
    
    private static final String TMPDIR = "/tmp";
    private static final String TESTNONCE = "testnonce";
    private static final int ALLOWEXPORT = 0;
    
    public TestSnapshotSaveonPartitions(String name){
        super(name);
    }
    @Override
    public void setUp()
    {
        deleteTestFiles();
        super.setUp();
    }
    @Override
    public void tearDown()
    {
        deleteTestFiles();
        try {
            super.tearDown();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    private void deleteTestFiles()
    {
        FilenameFilter cleaner = new FilenameFilter()
        {
            public boolean accept(File dir, String file)
            {
                return file.startsWith(TESTNONCE) ||
                file.endsWith(".vpt") ||
                file.endsWith(".digest") ||
                file.endsWith(".tsv") ||
                file.endsWith(".csv");
            }
        };

        File tmp_dir = new File(TMPDIR);
        File[] tmp_files = tmp_dir.listFiles(cleaner);
        for (File tmp_file : tmp_files)
        {
            tmp_file.delete();
        }
    }
    
    private void validateSnapshot(boolean expectSuccess) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        PrintStream original = System.out;
        try {
            System.setOut(ps);
            String args[] = new String[] {
                    TESTNONCE,
                    "--dir",
                    TMPDIR
            };
            SnapshotVerifier.main(args);
            ps.flush();
            String reportString = baos.toString("UTF-8");
            if (expectSuccess) {
                assertTrue(reportString.startsWith("Snapshot valid\n"));
            } else {
                assertTrue(reportString.startsWith("Snapshot corrupted\n"));
            }
        } catch (UnsupportedEncodingException e) {}
          finally {
            System.setOut(original);
        }
    }
    
    private VoltTable[] saveTables(Client client)
    {
        VoltTable[] results = null;
        try
        {
            results = client.callProcedure("SnapshotSave", TMPDIR,
                                           TESTNONCE,
                                           (byte)1).getResults();
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            fail("SnapshotSave exception: " + ex.getMessage());
        }
        return results;
    }
    public void testSnapshotSave() throws Exception {
        System.out.println("Starting testSnapshotSave");
        Client client = getClient();
        int num_replicated_items_per_chunk = 100;
        int num_replicated_chunks = 10;
        int num_partitioned_items_per_chunk = 120;
        int num_partitioned_chunks = 10;
        
        VoltTable[] results = null;

        results = client.callProcedure("@SnapshotSave", TMPDIR,
                                       TESTNONCE, (byte)1).getResults();
        validateSnapshot(true);
        
        VoltTable scanResults[] = client.callProcedure("@SnapshotScan", new Object[] { null }).getResults();
        assertNotNull(scanResults);
        assertEquals( 1, scanResults.length);
        assertEquals( 1, scanResults[0].getColumnCount());
        assertEquals( 1, scanResults[0].getRowCount());
        assertTrue( scanResults[0].advanceRow());
        assertTrue( "ERR_MSG".equals(scanResults[0].getColumnName(0)));

        scanResults = client.callProcedure("@SnapshotScan", "/doesntexist").getResults();
        assertNotNull(scanResults);
        assertEquals( 1, scanResults[1].getRowCount());
        assertTrue( scanResults[1].advanceRow());
        assertTrue( "FAILURE".equals(scanResults[1].getString("RESULT")));

        scanResults = client.callProcedure("@SnapshotScan", TMPDIR).getResults();
        assertNotNull(scanResults);
        assertEquals( 3, scanResults.length);
        assertEquals( 8, scanResults[0].getColumnCount());
        assertTrue(scanResults[0].getRowCount() >= 1);
        assertTrue(scanResults[0].advanceRow());
        
        int count = 0;
        String completeStatus = null;
        do {
            if (TESTNONCE.equals(scanResults[0].getString("NONCE"))) {
                assertTrue(TMPDIR.equals(scanResults[0].getString("PATH")));
                count++;
                completeStatus = scanResults[0].getString("COMPLETE");
            }
        } while (scanResults[0].advanceRow());
        assertEquals(1, count);
        assertNotNull(completeStatus);
        assertTrue("TRUE".equals(completeStatus));

        FilenameFilter cleaner = new FilenameFilter()
        {
            public boolean accept(File dir, String file)
            {
                return file.startsWith(TESTNONCE) && file.endsWith("vpt");
            }
        };

        File tmp_dir = new File(TMPDIR);
        File[] tmp_files = tmp_dir.listFiles(cleaner);
        tmp_files[0].delete();
        
        scanResults = client.callProcedure("@SnapshotScan", TMPDIR).getResults();
        assertNotNull(scanResults);
        assertEquals( 3, scanResults.length);
        assertEquals( 8, scanResults[0].getColumnCount());
        assertTrue(scanResults[0].getRowCount() >= 1);
        assertTrue(scanResults[0].advanceRow());
        
    }
    public void testSnapshot() throws IOException, ProcCallException,
    InterruptedException {
    Client client = this.getClient();
    ClientResponse cr = client.callProcedure("TestCount");
    assertEquals(Status.OK, cr.getStatus());
    VoltTable result = cr.getResults()[0];
    assertTrue(result.advanceRow());
    assertEquals(20, result.get(0, VoltType.INTEGER));

    }

}
