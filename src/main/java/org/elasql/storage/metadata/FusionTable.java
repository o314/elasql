package org.elasql.storage.metadata;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.elasql.sql.RecordKey;

public class FusionTable {
	
	private static final int DEFAULT_SIZE = 100_000;
	
	class LocationRecord {
		RecordKey key; // null => not used
		int partId;
		int nextFreeSlotId;
	}
	
	private int expMaxSize;
	private int size;
	private int firstFreeSlot;
	private LocationRecord[] locations;
	private Map<RecordKey, Integer> keyToSlotIds;
	private Map<RecordKey, Integer> overflowedKeys;
	private int lastReplacedSlot;
	private int[] countsPerParts = new int[PartitionMetaMgr.NUM_PARTITIONS];
	
	/**
	 * Create a fusion table with the expected maximum capacity.
	 * Note that the size of table may exceed the given size.
	 * The space only released when someone calls removedOverflowKeys.
	 * 
	 * @param expectedMaxSize
	 */
	public FusionTable(int expectedMaxSize) {
		if (expectedMaxSize == -1)
			expMaxSize = DEFAULT_SIZE;
		else
			expMaxSize = expectedMaxSize;
		size = 0;
		firstFreeSlot = 0;
		locations = new LocationRecord[expMaxSize];
		for (int i = 0; i < expMaxSize; i++) {
			locations[i] = new LocationRecord();
			if (i != expMaxSize - 1)
				locations[i].nextFreeSlotId = i + 1;
			else
				locations[i].nextFreeSlotId = -1;
		}
		keyToSlotIds = new HashMap<RecordKey, Integer>(expMaxSize);
		overflowedKeys = new HashMap<RecordKey, Integer>();
		lastReplacedSlot = 0;
		
//		new PeriodicalJob(5000, 600000, new Runnable() {
//			@Override
//			public void run() {
//				long time = System.currentTimeMillis() - Elasql.START_TIME_MS;
//				time /= 1000;
//				System.out.println(String.format("Time: %d seconds, Total Size: %d, Overflow Size: %d",
//						time, size, overflowedKeys.size()));
//			}
//		}).start();
		
//		new PeriodicalJob(5_000, 2400_000, new Runnable() {
//			@Override
//			public void run() {
//				long time = System.currentTimeMillis() - Elasql.START_TIME_MS;
//				time /= 1000;
//				
//				StringBuffer sb = new StringBuffer();
//				sb.append(String.format("Time: %d seconds - ", time));
//				for (int i = 0; i < countsPerParts.length; i++)
//					sb.append(String.format("%d, ", countsPerParts[i]));
//				sb.delete(sb.length() - 2, sb.length());
//				
//				System.out.println(sb.toString());
//			}
//		}).start();
	}
	
	public void setLocation(RecordKey key, int partId) {
		Integer slotId = keyToSlotIds.get(key);
		
		if (slotId != null) {
			countsPerParts[locations[slotId].partId]--;
			locations[slotId].partId = partId;
		} else {
			if (overflowedKeys.containsKey(key)) {
				countsPerParts[overflowedKeys.get(key)]--;
				overflowedKeys.put(key, partId);
			} else
				insertNewRecord(key, partId);
		}
		
		countsPerParts[partId]++;
	}
	
	public int getLocation(RecordKey key) {
		Integer slotId = keyToSlotIds.get(key);
		
		if (slotId != null)
			return locations[slotId].partId;
		else {
			Integer partId = overflowedKeys.get(key);
			if (partId != null)
				return partId;
			else
				return -1;
		}
	}
	
	public boolean containsKey(RecordKey key) {
		return keyToSlotIds.containsKey(key) || overflowedKeys.containsKey(key);
	}
	
	/**
	 * Remove the record of the location of the given key.
	 * 
	 * @param key
	 * @return the partition id in the record
	 */
	public int remove(RecordKey key) {
		Integer slotId = keyToSlotIds.remove(key);
		
		if (slotId != null) {
			locations[slotId].key = null;
			size--;
			countsPerParts[locations[slotId].partId]--;
			
			// add to the free slots
			locations[slotId].nextFreeSlotId = firstFreeSlot;
			firstFreeSlot = slotId;
			
			return locations[slotId].partId;
		} else {
			Integer partId = overflowedKeys.remove(key);
			if (partId != null) {
				size--;
				countsPerParts[partId]--;
				return partId;
			} else
				return -1;
		}
	}
	
	public int size() {
		return size;
	}
	
	@Deprecated
	public Map<RecordKey, Integer> removeOverflowKeys() {
		Map<RecordKey, Integer> removedKeys = overflowedKeys;
		overflowedKeys = new HashMap<RecordKey, Integer>();
		size -= removedKeys.size();
		return removedKeys;
	}
	
	public Set<RecordKey> getOverflowKeys() {
		return new HashSet<RecordKey>(overflowedKeys.keySet());
	}
	
	private void insertNewRecord(RecordKey key, int partId) {
		int freeSlot = findFreeSlot();
		if (freeSlot == -1)
			freeSlot = swapOutRecord();
		
		locations[freeSlot].key = key;
		locations[freeSlot].partId = partId;
		keyToSlotIds.put(key, freeSlot);
		size++;
	}
	
	private int findFreeSlot() {
		if (firstFreeSlot != -1) {
			int freeSlot = firstFreeSlot;
			firstFreeSlot = locations[freeSlot].nextFreeSlotId;
			return freeSlot;
		} else { // no free slot
			return -1;
		}
	}
	
	private int swapOutRecord() {
		int swapSlot = (lastReplacedSlot + 1) % expMaxSize;
		lastReplacedSlot = swapSlot;
		
		keyToSlotIds.remove(locations[swapSlot].key);
		overflowedKeys.put(locations[swapSlot].key, locations[swapSlot].partId);
		locations[swapSlot].key = null;
		
		return swapSlot;
	}
}