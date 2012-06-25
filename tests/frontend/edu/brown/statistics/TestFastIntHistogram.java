/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  http://hstore.cs.brown.edu/                                            *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/
package edu.brown.statistics;

import java.util.Collection;
import java.util.Random;

import org.json.JSONObject;

import edu.brown.BaseTestCase;

/**
 * 
 * @author pavlo
 */
public class TestFastIntHistogram extends BaseTestCase {

    public static final int NUM_PARTITIONS = 100;
    public static final int NUM_SAMPLES = 100;
    public static final int RANGE = 20;
    
    private Histogram<Integer> h = new Histogram<Integer>();
    private FastIntHistogram fast_h = new FastIntHistogram(RANGE);
    private Random rand = new Random(1);
    
    protected void setUp() throws Exception {
        // Cluster a bunch in the center
        int min = RANGE / 3;
        for (int i = 0; i < NUM_SAMPLES; i++) {
            int val = rand.nextInt(min) + min; 
            h.put(val);
            fast_h.fastPut(val);
        }
        for (int i = 0; i < NUM_SAMPLES; i++) {
            int val = rand.nextInt(RANGE); 
            h.put(val);
            fast_h.fastPut(val);
        }
    }
    
    /**
     * testSerialization
     */
    public void testSerialization() throws Exception {
        String json = fast_h.toJSONString();
        assertFalse(json.isEmpty());
        
        FastIntHistogram clone = new FastIntHistogram();
        JSONObject jsonObj = new JSONObject(json);
        clone.fromJSON(jsonObj, null);
        
        assertEquals(fast_h.fastSize(), clone.fastSize());
        for (int i = 0, cnt = fast_h.fastSize(); i < cnt; i++) {
            assertEquals(fast_h.fastGet(i), clone.fastGet(i));
        } // FOR
    }
    
    /**
     * testMinCount
     */
    public void testMinCount() throws Exception {
        assertEquals(h.getMinCount(), fast_h.getMinCount());
    }
    
    public void testMinValue() {
        assertEquals(h.getMinValue(), fast_h.getMinValue());
    }
    
    public void testMinCountValues() {
        Collection<Integer> vals0 = h.getMinCountValues();
        Collection<Integer> vals1 = fast_h.getMinCountValues();
        assertEquals(vals0.size(), vals1.size());
        assertTrue(vals0.containsAll(vals1));
    }
    
    /**
     * testMaxCount
     */
    public void testMaxCount() throws Exception {
        assertEquals(h.getMaxCount(), fast_h.getMaxCount());
    }
    
    public void testMaxValue() {
        assertEquals(h.getMaxValue(), fast_h.getMaxValue());
    }
    
    public void testMaxCountValues() {
        Collection<Integer> vals0 = h.getMaxCountValues();
        Collection<Integer> vals1 = fast_h.getMaxCountValues();
        assertEquals(vals0.size(), vals1.size());
        assertTrue(vals0.containsAll(vals1));
    }
    
    /**
     * testValues
     */
    public void testValues() throws Exception {
        Collection<Integer> vals0 = h.values();
        Collection<Integer> vals1 = fast_h.values();
        assertEquals(vals0.size(), vals1.size());
        assertTrue(vals0.containsAll(vals1));
    }
    
    /**
     * testValueCount
     */
    public void testValueCount() {
        assertEquals(h.getValueCount(), fast_h.getValueCount());
    }
    
    
    

}
