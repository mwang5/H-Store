/***************************************************************************
 *  Copyright (C) 2010 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Andy Pavlo (pavlo@cs.brown.edu)                                        *
 *  http://www.cs.brown.edu/~pavlo/                                        *
 *                                                                         *
 *  Visawee Angkanawaraphan (visawee@cs.brown.edu)                         *
 *  http://www.cs.brown.edu/~visawee/                                      *
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
package edu.brown.benchmark.auctionmark;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.catalog.Database;

import edu.brown.rand.AbstractRandomGenerator;
import edu.brown.rand.RandomDistribution.FlatHistogram;
import edu.brown.statistics.Histogram;
import edu.brown.utils.JSONSerializable;
import edu.brown.utils.JSONUtil;

public class AuctionMarkBenchmarkProfile implements JSONSerializable {
    protected static final Logger LOG = Logger.getLogger(AuctionMarkBaseClient.class.getName());
    
    public List<Long> user_ids;
    
    public enum Members {
        SCALE_FACTOR,
        TABLE_SIZES,
        ITEM_CATEGORY_HISTOGRAM,
        USER_IDS,
        USER_AVAILABLE_ITEMS, 
        USER_WAIT_FOR_PURCHASE_ITEMS,
        USER_COMPLETE_ITEMS,
        ITEM_BID_MAP,
        ITEM_BUYER_MAP,
        GAG_GAV_MAP,
        GAG_GAV_HISTOGRAM
    };
    
    /**
     * Data Scale Factor
     */
    public long scale_factor;
    
    /**
     * Map from table names to the number of tuples we inserted during loading
     */
    public SortedMap<String, Long> table_sizes = new TreeMap<String, Long>();
    
    /**
     * Histogram for number of items per category (stored as category_id)
     */
    public Histogram item_category_histogram = new Histogram();

    /**
     * Three status types for an item
     * 1. Available (The auction of this item is still open)
     * 2. Wait for purchase
     * 		- The auction of this item is still open. There is a bid winner and the bid winner has not purchased the item.
     * 3. Complete (The auction is closed and (There is no bid winner or The bid winner has already purchased the itme))
     */
    public Histogram user_available_items_histogram;
    public Histogram user_wait_for_purchase_items_histogram;
    public Histogram user_complete_items_histogram;
    
    public Map<Long, List<Long>> user_available_items;
    public Map<Long, List<Long>> user_wait_for_purchase_items;
    public Map<Long, List<Long>> user_complete_items;
    public Map<Long, Long> item_bid_map;
    public Map<Long, Long> item_buyer_map;
        
    // Map from global attribute group to list of global attribute value
    public Map<Long, List<Long>> gag_gav_map;
    public Histogram gag_gav_histogram;
    
    // Map from user ID to total number of items that user sell
    //public Map<Long, Integer> user_total_items = new ConcurrentHashMap<Long, Integer>();
    
    // -----------------------------------------------------------------
    // GENERAL METHODS
    // -----------------------------------------------------------------

    /**
     * Constructor - Keep your pimp hand strong!
     */
    public AuctionMarkBenchmarkProfile() {
        
        // Initialize table sizes
        for (String tableName : AuctionMarkConstants.TABLENAMES) {
            this.table_sizes.put(tableName, 0l);
        }
        
        //_lastUserId = this.getTableSize(AuctionMarkConstants.TABLENAME_USER);
        
        LOG.debug("AuctionMarkBenchmarkProfile :: constructor");
        
        user_ids = new ArrayList<Long>();
        
        user_available_items = new ConcurrentHashMap<Long, List<Long>>();
        user_available_items_histogram = new Histogram();
        
        user_wait_for_purchase_items = new ConcurrentHashMap<Long, List<Long>>();
        user_wait_for_purchase_items_histogram = new Histogram();
        
        user_complete_items = new ConcurrentHashMap<Long, List<Long>>();
        user_complete_items_histogram = new Histogram();
        
        item_bid_map = new ConcurrentHashMap<Long, Long>();
        item_buyer_map = new ConcurrentHashMap<Long, Long>();
        
        gag_gav_map = new ConcurrentHashMap<Long, List<Long>>();
        gag_gav_histogram = new Histogram();
    }
    
    /**
     * Get the scale factor value for this benchmark profile
     * @return
     */
    public long getScaleFactor() {
        return (this.scale_factor);
    }
    
    /**
     * Set the scale factor for this benchmark profile
     * @param scale_factor
     */
    public void setScaleFactor(long scale_factor) {
        assert(scale_factor > 0) : "Invalid scale factor " + scale_factor;
        this.scale_factor = scale_factor;
    }
    
    public long getTableSize(String table_name) {
        return (this.table_sizes.get(table_name));
    }
    
    public void setTableSize(String table_name, long size) {
        this.table_sizes.put(table_name, size);
    }
    
    /**
     * Add the give tuple to the running to total for the table
     * @param table_name
     * @param size
     */
    public void addToTableSize(String table_name, long size) {
        Long orig_size = this.table_sizes.get(table_name);
        if (orig_size == null) orig_size = 0l;
        this.setTableSize(table_name, orig_size + size);
    }
    
    
    public void addUserId(long userId){
    	LOG.debug("@@@ adding userId = " + userId);
    	user_ids.add(userId);
    }
    
    public long getUserId(int index){
    	return user_ids.get(index);
    }
    
    public List<Long> getUserIds(){
    	return user_ids;
    }
    
    /*
     * Available item manipulators
     * 
     */    
    public void addAvailableItem(long sellerId, long itemId){
    	synchronized(user_available_items){
	    	List<Long> itemList = user_available_items.get(sellerId);
	    	if(null == itemList){
	    		itemList = new LinkedList<Long>();
	    		itemList.add(itemId);
	    		user_available_items.put(sellerId, itemList);
	    		user_available_items_histogram.put(sellerId);
	    	} else if(!itemList.contains(itemId)){
	    		itemList.add(itemId);
	    		user_available_items_histogram.put(sellerId);
	    	}
    	}
    }
    
    public synchronized void removeAvailableItem(long sellerId, long itemId){
    	synchronized(user_available_items){
	    	List<Long> itemList = user_available_items.get(sellerId);
	    	if(null != itemList && itemList.remove(new Long(itemId))){
	    		user_available_items_histogram.remove(sellerId, 1);
	    		if(0 == itemList.size()){
	    			user_available_items.remove(sellerId);
	    		}
	    	}
    	}
    }
    
    public synchronized Long[] getRandomAvailableItemIdSellerIdPair(AbstractRandomGenerator rng){
    	synchronized(user_available_items){
	    	FlatHistogram randomSeller = new FlatHistogram(rng, user_available_items_histogram);
	    	Long sellerId = randomSeller.nextLong();
	    	long numAvailableItems = user_available_items_histogram.get(sellerId);
	    	Long itemId = user_available_items.get(sellerId).get(rng.number(0, (int)numAvailableItems - 1));
	    	Long[] ret = {itemId, sellerId};
			return ret;
    	}
    }
    
    /*
     * Complete item manipulators
     *  
     */
    
    public void addCompleteItem(long sellerId, long itemId){
    	synchronized(user_complete_items){
	    	List<Long> itemList = user_complete_items.get(sellerId);
	    	if(null == itemList){
	    		itemList = new LinkedList<Long>();
	    		itemList.add(itemId);
	    		user_complete_items.put(sellerId, itemList);
	    		user_complete_items_histogram.put(sellerId);
	    	} else if(!itemList.contains(itemId)){
	    		itemList.add(itemId);
	    		user_complete_items_histogram.put(sellerId);
	    	}
    	}
    }
    
    public Long[] getRandomCompleteItemIdSellerIdPair(AbstractRandomGenerator rng){
    	synchronized(user_complete_items){
	    	FlatHistogram randomSeller = new FlatHistogram(rng, user_complete_items_histogram);
	    	Long sellerId = randomSeller.nextLong();
	    	long numCompleteItems = user_complete_items_histogram.get(sellerId);
	    	Long itemId = user_complete_items.get(sellerId).get(rng.number(0, (int)numCompleteItems - 1));
	    	Long[] ret = {itemId, sellerId};
			return ret;
    	}
    }
    
    public void addWaitForPurchaseItem(long sellerId, long itemId, long bidId, long buyerId){
    	synchronized(user_wait_for_purchase_items){
	    	List<Long> itemList = user_wait_for_purchase_items.get(sellerId);
	    	item_bid_map.put(itemId, bidId);
	    	item_buyer_map.put(itemId, buyerId);
	    	if(null == itemList){
	    		itemList = new LinkedList<Long>();
	    		itemList.add(itemId);
	    		user_wait_for_purchase_items.put(sellerId, itemList);
	    		user_wait_for_purchase_items_histogram.put(sellerId);
	    	} else if(!itemList.contains(itemId)){
	    		itemList.add(itemId);
	    		user_wait_for_purchase_items_histogram.put(sellerId);
	    	}
    	}
    }

    public void removeWaitForPurchaseItem(long sellerId, long itemId){
    	synchronized(user_wait_for_purchase_items){
    		List<Long> itemList = user_wait_for_purchase_items.get(sellerId);
	    	if(null != itemList && itemList.remove(new Long(itemId))){
	    		user_wait_for_purchase_items_histogram.remove(sellerId, 1);
	    		if(0 == itemList.size()){
	    			user_wait_for_purchase_items.remove(sellerId);
	    			item_bid_map.remove(itemId);
	    			item_buyer_map.remove(itemId);
	    		}
	    	}
    	}
    }
    
    public Long[] getRandomWaitForPurchaseItemIdSellerIdPair(AbstractRandomGenerator rng){
    	synchronized(user_wait_for_purchase_items){
	    	FlatHistogram randomSeller = new FlatHistogram(rng, user_wait_for_purchase_items_histogram);
	    	Long sellerId = randomSeller.nextLong();
	    	long numWaitForPurchaseItems = user_wait_for_purchase_items.get(sellerId).size();
	    	Long itemId = user_wait_for_purchase_items.get(sellerId).get(rng.number(0, (int)numWaitForPurchaseItems - 1));
	    	Long[] ret = {itemId, sellerId};
			return ret;
    	}
    }
    
    public long getBidId(long itemId){
    	return item_bid_map.get(itemId);
    }
    
    public long getBuyerId(long itemId){
    	return item_buyer_map.get(itemId);
    }
    
    
    /**
     * Gets random buyer ID who has a bid in "Wait For Purchase" status.
     * Note that this method will decrement the number of bids in 
     * "Wait For Purchase" status of a buyer whose ID is return.
     * 
     * @param rng the random generator
     * @return random buyer ID who has a bid in the "Wait For Purchase" status.
     */
    /*
    public Long getRandomBuyerWhoHasWaitForPurchaseBid(AbstractRandomGenerator rng){
    	int randomIndex = rng.number(0, buyer_num_wait_for_purchase_bids.size() - 1);
    	int i=0;
    	Long buyerId = null;
    	int numBids = 0;
    	
    	for(Map.Entry<Long, Integer> entry: buyer_num_wait_for_purchase_bids.entrySet()){
    		if(i++ == randomIndex){
    			buyerId = entry.getKey();
    			numBids = entry.getValue();
    			break;
    		}
    	}
    	
    	if(numBids > 1){
    		buyer_num_wait_for_purchase_bids.put(buyerId, numBids - 1);
    	} else {
    		buyer_num_wait_for_purchase_bids.remove(buyerId);
    	}
    	
    	return buyerId;
    }
    */
    
    /**
     * Increments the number of bids in the "Wait For Purchase" status
     * of a given buyerId.
     * 
     * @param buyerId
     */
    /*
    public void addBuyerWhoHasWaitForPurchaseBid(Long buyerId){
    	int numBids;
    	if(buyer_num_wait_for_purchase_bids.containsKey(buyerId)){
    		numBids = buyer_num_wait_for_purchase_bids.get(buyerId) + 1;
    	} else {
    		numBids = 1;
    	}
    	buyer_num_wait_for_purchase_bids.put(buyerId, numBids);
    }
    */
    
    
    public void addGAGIdGAVIdPair(long GAGId, long GAVId){
    	List<Long> GAVIds = gag_gav_map.get(GAGId);
    	if(null == GAVIds){
    		GAVIds = new ArrayList<Long>();
    		gag_gav_map.put(GAGId, GAVIds);
    	} else if(GAVIds.contains(GAGId)){
    		return;	
    	}
    	GAVIds.add(GAVId);
		gag_gav_histogram.put(GAGId);
    }
    
    public Long[] getRandomGAGIdGAVIdPair(AbstractRandomGenerator rng){
    	
    	FlatHistogram randomGAGId = new FlatHistogram(rng, gag_gav_histogram);
    	Long GAGId = randomGAGId.nextLong();
    	
    	List<Long> GAVIds = gag_gav_map.get(GAGId);
    	Long GAVId = GAVIds.get(rng.nextInt(GAVIds.size())); 
    	
    	return new Long[]{GAGId, GAVId};
    }
    
    public long getRandomCategoryId(AbstractRandomGenerator rng){
    	FlatHistogram randomCategory = new FlatHistogram(rng, item_category_histogram);
    	return randomCategory.nextLong();
    }
    
    /*
    public void incrementTotalItems(long user_id) {
    	synchronized(user_total_items){
	    	Integer totalItems = user_total_items.get(user_id);
	    	if(null == totalItems){
	    		totalItems = 1;
	    	} else {
	    		totalItems++;
	    	}
	    	user_total_items.put(user_id, totalItems);
    	}
    }
    
    public int getTotalItems(long user_id) {
    	synchronized(user_total_items){
	    	if(user_total_items.containsKey(user_id)){
		    	return user_total_items.get(user_id);
		    } else {
		    	return 0;
		    }
    	}
    }
    */
    /*
    
    public void setItemsPerUser(long user_id, int total_items, int completed_items) {
        // TODO (pavlo)
    }
    
    
    
    public void setCompletedItemsPerUser(long user_id, int completed_items) {
    	// TODO (pavlo)
    }
    
    
    
    public int getCompletedItems(long user_id) {
        return 0; // TODO(pavlo)
    }

    public int getAvailableItems(long user_id) {
        return 0; // TODO(pavlo)
    }
    */
    
    //public
    
    // -----------------------------------------------------------------
    // SERIALIZATION
    // -----------------------------------------------------------------
    
    @Override
    public void load(String input_path, Database catalog_db) throws IOException {
        JSONUtil.load(this, catalog_db, input_path);
    }
    
    @Override
    public void save(String output_path) throws IOException {
        JSONUtil.save(this, output_path);
    }
    
    @Override
    public String toJSONString() {
        return (JSONUtil.toJSONString(this));
    }
    
    @Override
    public void toJSON(JSONStringer stringer) throws JSONException {
        JSONUtil.fieldsToJSON(stringer, this, AuctionMarkBenchmarkProfile.class, AuctionMarkBenchmarkProfile.Members.values());
    }
    
    @Override
    public void fromJSON(JSONObject json_object, Database catalog_db) throws JSONException {
        JSONUtil.fieldsFromJSON(json_object, catalog_db, this, AuctionMarkBenchmarkProfile.class, AuctionMarkBenchmarkProfile.Members.values());
    }
}