/* This file is part of VoltDB.
* Copyright (C) 2008-2010 VoltDB Inc.
*
* Permission is hereby granted, free of charge, to any person obtaining
* a copy of this software and associated documentation files (the
* "Software"), to deal in the Software without restriction, including
* without limitation the rights to use, copy, modify, merge, publish,
* distribute, sublicense, and/or sell copies of the Software, and to
* permit persons to whom the Software is furnished to do so, subject to
* the following conditions:
*
* The above copyright notice and this permission notice shall be
* included in all copies or substantial portions of the Software.
*
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
* EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
* MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
* IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
* OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
* ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
* OTHER DEALINGS IN THE SOFTWARE.
*/

package org.voltdb.regressionsuites;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import org.voltdb.BackendTarget;
import org.voltdb.DefaultSnapshotDataTarget;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Table;
import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;
import org.voltdb.utils.SnapshotVerifier;
import org.voltdb.utils.SnapshotConverter;
import org.voltdb.regressionsuites.saverestore.CatalogChangeSingleProcessServer;
import org.voltdb.regressionsuites.saverestore.SaveRestoreTestProjectBuilder;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.HStore;

/**
* Test the SnapshotSave and SnapshotRestore system procedures
*/
public class TestSaveRestoreSysprocSuite extends RegressionSuite {

    private static final String TMPDIR = "/tmp";
    private static final String TESTNONCE = "testnonce";
    private static final int ALLOWEXPORT = 0;

    public TestSaveRestoreSysprocSuite(String name) 
    {
        super(name);
    }

    @Override
    public void setUp() 
    {
        deleteTestFiles();
        super.setUp();
        DefaultSnapshotDataTarget.m_simulateFullDiskWritingChunk = false;
        DefaultSnapshotDataTarget.m_simulateFullDiskWritingHeader = false;
    }

    @Override
    public void tearDown() throws Exception
    {
        deleteTestFiles();
        super.tearDown();
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
    
    private void corruptTestFiles(java.util.Random r) throws Exception
    {
        FilenameFilter cleaner = new FilenameFilter()
        {
            public boolean accept(File dir, String file)
            {
                return file.startsWith(TESTNONCE);
            }
        };

        File tmp_dir = new File(TMPDIR);
        File[] tmp_files = tmp_dir.listFiles(cleaner);
        int tmpIndex = r.nextInt(tmp_files.length);
        byte corruptValue[] = new byte[1];
        r.nextBytes(corruptValue);
        java.io.RandomAccessFile raf = new java.io.RandomAccessFile( tmp_files[tmpIndex], "rw");
        int corruptPosition = r.nextInt((int)raf.length());
        raf.seek(corruptPosition);
        byte currentValue = raf.readByte();
        while (currentValue == corruptValue[0]) {
            r.nextBytes(corruptValue);
        }
        System.out.println("Corrupting file " + tmp_files[tmpIndex].getName() +
                " at byte " + corruptPosition + " with value " + corruptValue[0]);
        raf.seek(corruptPosition);
        raf.write(corruptValue);
        raf.close();
    }
    
    private VoltTable createReplicatedTable(int numberOfItems,
            int indexBase,
            StringBuilder sb) {
        return createReplicatedTable(numberOfItems, indexBase, sb, false);
    }
    
    private VoltTable createReplicatedTable(int numberOfItems,
                                            int indexBase,
                                            StringBuilder sb,
                                            boolean generateCSV)
    {
        VoltTable repl_table =
            new VoltTable(new ColumnInfo("RT_ID", VoltType.INTEGER),
                          new ColumnInfo("RT_NAME", VoltType.STRING),
                          new ColumnInfo("RT_INTVAL", VoltType.INTEGER),
                          new ColumnInfo("RT_FLOATVAL", VoltType.FLOAT));
        char delimeter = generateCSV ? ',' : '\t';
        for (int i = indexBase; i < numberOfItems + indexBase; i++) {
            String stringVal = null;
            String escapedVal = null;

            if (sb != null) {
                if (generateCSV) {
                    int escapable = i % 5;
                    switch (escapable) {
                    case 0:
                        stringVal = "name_" + i;
                        escapedVal = "name_" + i;
                        break;
                    case 1:
                        stringVal = "na,me_" + i;
                        escapedVal = "\"na,me_" + i + "\"";
                        break;
                    case 2:
                        stringVal = "na\"me_" + i;
                        escapedVal = "\"na\"\"me_" + i + "\"";
                        break;
                    case 3:
                        stringVal = "na\rme_" + i;
                        escapedVal = "\"na\rme_" + i + "\"";
                        break;
                    case 4:
                        stringVal = "na\nme_" + i;
                        escapedVal = "\"na\nme_" + i + "\"";
                        break;
                    }
                } else {
                    int escapable = i % 5;
                    switch (escapable) {
                    case 0:
                        stringVal = "name_" + i;
                        escapedVal = "name_" + i;
                        break;
                    case 1:
                        stringVal = "na\tme_" + i;
                        escapedVal = "na\\tme_" + i;
                        break;
                    case 2:
                        stringVal = "na\nme_" + i;
                        escapedVal = "na\\nme_" + i;
                        break;
                    case 3:
                        stringVal = "na\rme_" + i;
                        escapedVal = "na\\rme_" + i;
                        break;
                    case 4:
                        stringVal = "na\\me_" + i;
                        escapedVal = "na\\\\me_" + i;
                        break;
                    }
                }
            } else {
                stringVal = "name_" + i;
            }

            Object[] row = new Object[] {i,
                                         stringVal,
                                         i,
                                         new Double(i)};
            if (sb != null) {
                sb.append(i).append(delimeter).append(escapedVal).append(delimeter);
                sb.append(i).append(delimeter).append(new Double(i).toString()).append('\n');
            }
            repl_table.addRow(row);
        }
        return repl_table;
    }

    private VoltTable createPartitionedTable(int numberOfItems, int indexBase)
    {
        VoltTable partition_table =
                new VoltTable(new ColumnInfo("PT_ID", VoltType.INTEGER),
                              new ColumnInfo("PT_NAME", VoltType.STRING),
                              new ColumnInfo("PT_INTVAL", VoltType.INTEGER),
                              new ColumnInfo("PT_FLOATVAL", VoltType.FLOAT));

        for (int i = indexBase; i < numberOfItems + indexBase; i++)
        {
            Object[] row = new Object[] {i,
                                         "name_" + i,
                                         i,
                                         new Double(i)};
            partition_table.addRow(row);
        }
        return partition_table;
    }

    private VoltTable[] loadTable(Client client, String tableName, VoltTable table)
    {
        VoltTable[] results = null;
        try
        {
            client.callProcedure("@LoadMultipartitionTable", tableName, table);
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            fail("loadTable exception: " + ex.getMessage());
        }
        return results;
    }

    private void loadLargeReplicatedTable(Client client, String tableName,
            int itemsPerChunk, int numChunks) {
        loadLargeReplicatedTable(client, tableName, itemsPerChunk, numChunks, false, null);
    }

    private void loadLargeReplicatedTable(Client client, String tableName,
                                          int itemsPerChunk, int numChunks, boolean generateCSV, StringBuilder sb)
    {
        for (int i = 0; i < numChunks; i++)
        {
            VoltTable repl_table =
                createReplicatedTable(itemsPerChunk, i * itemsPerChunk, sb, generateCSV);
            loadTable(client, tableName, repl_table);
        }
        if (sb != null) {
            sb.trimToSize();
        }
    }

    private VoltTable[] saveTables(Client client)
    {
        VoltTable[] results = null;
        try
        {
            results = client.callProcedure("@SnapshotSave", TMPDIR,
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
    
    private void checkTable(Client client, String tableName, 
            String orderByCol, int expectedRows)
    {
        if (expectedRows > 200000)
        {
            System.out.println("Table too large to retrieve with select *");
            System.out.println("Skipping integrity check");
        }
        VoltTable result = null;
        try
        {
            result = client.callProcedure("SaveRestoreSelect", tableName).getResults()[0];
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        final int rowCount = result.getRowCount();
        assertEquals(expectedRows, rowCount);

        int i = 0;
        while (result.advanceRow())
        {
            assertEquals(i, result.getLong(0));
            assertEquals("name_" + i, result.getString(1));
            assertEquals(i, result.getLong(2));
            assertEquals(new Double(i), result.getDouble(3));
            ++i;
        }
    }
    
    private void loadLargePartitionedTable(Client client, String tableName,
                                          int itemsPerChunk, int numChunks)
    {
        for (int i = 0; i < numChunks; i++)
        {
            VoltTable part_table =
                createPartitionedTable(itemsPerChunk, i * itemsPerChunk);
            loadTable(client, tableName, part_table);
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

    public void testSaveRestoreJumboRows()
    throws IOException, InterruptedException, ProcCallException
    {
        System.out.println("Starting testSaveRestoreJumboRows.");
        Client client = getClient();
        byte firstStringBytes[] = new byte[1048576];
        java.util.Arrays.fill(firstStringBytes, (byte)'c');
        String firstString = new String(firstStringBytes, "UTF-8");
        byte secondStringBytes[] = new byte[1048564];
        java.util.Arrays.fill(secondStringBytes, (byte)'a');
        String secondString = new String(secondStringBytes, "UTF-8");

        VoltTable results[] = client.callProcedure("JumboInsert", 0, firstString, secondString).getResults();
        firstString = null;
        secondString = null;

        assertEquals(results.length, 1);
        assertEquals( 1, results[0].asScalarLong());

        results = client.callProcedure("JumboSelect", 0).getResults();
        assertEquals(results.length, 1);
        assertTrue(results[0].advanceRow());
        assertTrue(java.util.Arrays.equals( results[0].getStringAsBytes(1), firstStringBytes));
        assertTrue(java.util.Arrays.equals( results[0].getStringAsBytes(2), secondStringBytes));

        saveTables(client);
        validateSnapshot(true);

        // Kill and restart all the execution sites.
        m_config.shutDown();

        releaseClient(client);
        // Kill and restart all the execution sites.
        m_config.shutDown();
        m_config.startUp();

        client = getClient();

//        client.callProcedure("@SnapshotRestore", TMPDIR, TESTNONCE, ALLOWEXPORT);
//
//        results = client.callProcedure("JumboSelect", 0).getResults();
//        assertEquals(results.length, 1);
//        assertTrue(results[0].advanceRow());
//        assertTrue(java.util.Arrays.equals( results[0].getStringAsBytes(1), firstStringBytes));
//        assertTrue(java.util.Arrays.equals( results[0].getStringAsBytes(2), secondStringBytes));
    }
    
    /*
    * Also does some basic smoke tests
    * of @SnapshotSave, @SnapshotScan
    */
    public void testSnapshotSave() throws Exception
    {
        System.out.println("Starting testSnapshotSave");
        Client client = this.getClient();

        int num_replicated_items_per_chunk = 100;
        int num_replicated_chunks = 10;
        int num_partitioned_items_per_chunk = 120;
        int num_partitioned_chunks = 10;

        loadLargeReplicatedTable(client, "REPLICATED_TESTER",
                                 num_replicated_items_per_chunk,
                                 num_replicated_chunks);
        loadLargePartitionedTable(client, "PARTITION_TESTER",
                                  num_partitioned_items_per_chunk,
                                  num_partitioned_chunks);

        VoltTable[] results = null;
        results = client.callProcedure("@SnapshotSave", TMPDIR, TESTNONCE, (byte)1).getResults();
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
        assertEquals(1, scanResults[1].getRowCount());
        assertTrue( scanResults[1].advanceRow());
        assertTrue( "FAILURE".equals(scanResults[1].getString("RESULT")));

        scanResults = client.callProcedure("@SnapshotScan", TMPDIR).getResults();
        assertNotNull(scanResults);
        assertEquals( 3, scanResults.length);
        assertEquals( 8, scanResults[0].getColumnCount());
        assertTrue(scanResults[1].getRowCount() >= 1);
        assertTrue(scanResults[1].advanceRow());

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
        assertTrue(scanResults[1].getRowCount() >= 1);
        assertTrue(scanResults[1].advanceRow());

        // Instead of something exhaustive, let's just make sure that we get
        // the number of result rows corresponding to the number of ExecutionSites
        // that did save work
        Cluster cluster = CatalogUtil.getCluster(HStore.instance().getCatalog());
        Database database = cluster.getDatabases().get("database");
        CatalogMap<Table> tables = database.getTables();
        CatalogMap<Site> sites = cluster.getSites();
        int num_hosts = cluster.getHosts().size();
        int replicated = 0;
        int total_tables = 0;
        int expected_entries = 0;

        for (Table table : tables)
        {
            // Ignore materialized tables
            if (table.getMaterializer() == null)
            {
                total_tables++;
                if (table.getIsreplicated())
                {
                    replicated++;
                }
            }
        }

        for (Site s : sites) {
            if (s.getIsup()) {
                expected_entries++;
            }
        }

        expected_entries =
            ((total_tables - replicated) * num_hosts) + replicated;
        try
        {
            results = client.callProcedure("@SnapshotSave", TMPDIR,
                                           TESTNONCE, (byte)1).getResults(); 
        }
       
        catch (Exception ex)
        {
            ex.printStackTrace();
            fail("SnapshotSave exception: " + ex.getMessage());
        }
        assertEquals(expected_entries, results[0].getRowCount());
        while (results[0].advanceRow())
        {
            if (!tmp_files[0].getName().contains(results[0].getString("TABLE"))) {
                assertEquals(results[0].getString("RESULT"), "FAILURE");
                assertTrue(results[0].getString("ERR_MSG").contains("SAVE FILE ALREADY EXISTS"));
            }
        }
        
        /*snapshotdelete was not implemented*/
    }
    private void generateAndValidateTextFile(StringBuilder expectedText, boolean csv) throws Exception {
        String args[] = new String[] {
                TESTNONCE,
               "--dir",
               TMPDIR,
               "--table",
               "REPLICATED_TESTER",
               "--type",
               csv ? "CSV" : "TSV",
               "--outdir",
               TMPDIR
        };
        SnapshotConverter.main(args);
        FileInputStream fis = new FileInputStream(
                TMPDIR + File.separator + "REPLICATED_TESTER" + (csv ? ".csv" : ".tsv"));
        try {
            int filesize = (int)fis.getChannel().size();
            ByteBuffer expectedBytes = ByteBuffer.wrap(expectedText.toString().getBytes("UTF-8"));
            ByteBuffer readBytes = ByteBuffer.allocate(filesize);
            while (readBytes.hasRemaining()) {
                int read = fis.getChannel().read(readBytes);
                if (read == -1) {
                    throw new EOFException();
                }
            }
            // this throws an exception on failure
            new String(readBytes.array(), "UTF-8");

            readBytes.flip();
            assertTrue(expectedBytes.equals(readBytes));
        } finally {
            fis.close();
        }
    }

    // Test that we fail properly when there are no savefiles available
    public void testRestoreMissingFiles()
    throws IOException, InterruptedException
    {
        System.out.println("Starting testRestoreMissingFile");
        int num_replicated_items = 1000;
        int num_partitioned_items = 126;

        Client client = getClient();

        VoltTable repl_table = createReplicatedTable(num_replicated_items, 0, null);
        // make a TPCC warehouse table
        VoltTable partition_table =
            createPartitionedTable(num_partitioned_items, 0);

        loadTable(client, "REPLICATED_TESTER", repl_table);
        loadTable(client, "PARTITION_TESTER", partition_table);
        saveTables(client);

        validateSnapshot(true);

        // Kill and restart all the execution sites.
        m_config.shutDown();
        deleteTestFiles();
        m_config.startUp();

        client = getClient();

        try {
            client.callProcedure("@SnapshotRestore", TMPDIR, TESTNONCE, ALLOWEXPORT);
        }
        catch (Exception e) {
            assertTrue(e.getMessage().contains("No savefile state to restore"));
            return;
        }
        assertTrue(false);
    }

    // Test that we fail properly when the save files are corrupted
//    public void testCorruptedFiles()
//    throws Exception
//    {
//        System.out.println("Starting testCorruptedFiles");
//        int num_replicated_items = 1000;
//        int num_partitioned_items = 126;
//        java.util.Random r = new java.util.Random(0);
//        final int iterations = isValgrind() ? 5 : 100;
//
//        for (int ii = 0; ii < iterations; ii++) {
//            Client client = getClient();
//            VoltTable repl_table = createReplicatedTable(num_replicated_items, 0, null);
//            // make a TPCC warehouse table
//            VoltTable partition_table =
//                createPartitionedTable(num_partitioned_items, 0);
//
//            loadTable(client, "REPLICATED_TESTER", repl_table);
//            loadTable(client, "PARTITION_TESTER", partition_table);
//            VoltTable results[] = saveTables(client);
//            validateSnapshot(true);
//            while (results[0].advanceRow()) {
//                if (results[0].getString("RESULT").equals("FAILURE")) {
//                    System.out.println(results[0].getString("ERR_MSG"));
//                }
//                assertTrue(results[0].getString("RESULT").equals("SUCCESS"));
//            }
//
//            corruptTestFiles(r);
//            validateSnapshot(false);
//            releaseClient(client);
//            // Kill and restart all the execution sites.
//            m_config.shutDown();
//            m_config.startUp();
//
//            client = getClient();
//
//            results = client.callProcedure("@SnapshotRestore", TMPDIR, TESTNONCE, ALLOWEXPORT).getResults();
//            assertNotNull(results);
//            deleteTestFiles();
//            releaseClient(client);
//        }
//    }
//
//    // Test that a random corruption doesn't mess up the table. Not reproducible but useful for detecting
//    // stuff we won't normally find
//    public void testCorruptedFilesRandom()
//    throws Exception
//    {
//        System.out.println("Starting testCorruptedFilesRandom");
//        int num_replicated_items = 1000;
//        int num_partitioned_items = 126;
//        java.util.Random r = new java.util.Random();
//        final int iterations = isValgrind() ? 5 : 100;
//
//        for (int ii = 0; ii < iterations; ii++) {
//            Client client = getClient();
//
//            VoltTable repl_table = createReplicatedTable(num_replicated_items, 0, null);
//            // make a TPCC warehouse table
//            VoltTable partition_table =
//                createPartitionedTable(num_partitioned_items, 0);
//
//            loadTable(client, "REPLICATED_TESTER", repl_table);
//            loadTable(client, "PARTITION_TESTER", partition_table);
//            saveTables(client);
//            validateSnapshot(true);
//            releaseClient(client);
//            // Kill and restart all the execution sites.
//            m_config.shutDown();
//            corruptTestFiles(r);
//            validateSnapshot(false);
//            m_config.startUp();
//
//            client = getClient();
//
//            VoltTable results[] = client.callProcedure("@SnapshotRestore", TMPDIR, TESTNONCE, ALLOWEXPORT).getResults();
//            assertNotNull(results);
//            deleteTestFiles();
//            releaseClient(client);
//        }
//    }
//    
//    public void testRepartition()
//    throws IOException, InterruptedException, ProcCallException
//    {
//        System.out.println("Starting testRepartition");
//        int num_replicated_items_per_chunk = 100;
//        int num_replicated_chunks = 10;
//        int num_partitioned_items_per_chunk = 120; // divisible by 3 and 4
//        int num_partitioned_chunks = 10;
//        Client client = getClient();
//    
//        loadLargeReplicatedTable(client, "REPLICATED_TESTER",
//                                 num_replicated_items_per_chunk,
//                                 num_partitioned_chunks);
//        loadLargePartitionedTable(client, "PARTITION_TESTER",
//                                  num_partitioned_items_per_chunk,
//                                  num_partitioned_chunks);
//        VoltTable[] results = null;
//        results = saveTables(client);
//        validateSnapshot(true);
//        // Kill and restart all the execution sites.
//        m_config.shutDown();
//    
//        CatalogChangeSingleProcessServer config =
//            (CatalogChangeSingleProcessServer) m_config;
//        config.recompile(4);
//    
//        m_config.startUp();
//    
//        client = getClient();
//    
//        try
//        {
//            results = client.callProcedure("@SnapshotRestore", TMPDIR,
//                                           TESTNONCE, ALLOWEXPORT).getResults();
//            // XXX Should check previous results for success but meh for now
//        }
//        catch (Exception ex)
//        {
//            ex.printStackTrace();
//            fail("SnapshotRestore exception: " + ex.getMessage());
//        }
//    
//        checkTable(client, "PARTITION_TESTER", "PT_ID",
//                   num_partitioned_items_per_chunk * num_partitioned_chunks);
//        checkTable(client, "REPLICATED_TESTER", "RT_ID",
//                   num_replicated_items_per_chunk * num_replicated_chunks);
//    
//        results = client.callProcedure("@Statistics", "table", 0).getResults();
//    
//        int foundItem = 0;
//        while (results[0].advanceRow())
//        {
//            if (results[0].getString("TABLE_NAME").equals("PARTITION_TESTER"))
//            {
//                ++foundItem;
//                assertEquals((num_partitioned_items_per_chunk * num_partitioned_chunks) / 4,
//                        results[0].getLong("TABLE_ACTIVE_TUPLE_COUNT"));
//            }
//        }
//        // make sure all sites were loaded
//        assertEquals(4, foundItem);
//    
//        config.revertCompile();
//    }
    
    /**
    * Build a list of the tests to be run. Use the regression suite
    * helpers to allow multiple back ends.
    * JUnit magic that uses the regression suite helper classes.
    */
    static public junit.framework.Test suite() {
        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestSaveRestoreSysprocSuite.class);
        SaveRestoreTestProjectBuilder project =
            new SaveRestoreTestProjectBuilder();  
        VoltServerConfig config = null;

          project.addAllDefaults();
          
        config =
            new LocalSingleProcessServer("sysproc-threesites.jar", 1,
                                                 BackendTarget.NATIVE_EE_JNI);
        boolean success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        return builder;
    }
}