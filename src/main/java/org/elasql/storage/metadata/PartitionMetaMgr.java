/*******************************************************************************
 * Copyright 2016 vanilladb.org
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.elasql.storage.metadata;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.elasql.sql.RecordKey;
import org.elasql.util.ElasqlProperties;

public abstract class PartitionMetaMgr {
	
	public final static boolean LOAD_METIS_PARTITIONS = false;
	public final static int METIS_DATA_RANGE = 10;

	public final static int NUM_PARTITIONS;
//	public final static File LOGDIR;
//	public final static File LOGFILE;
	public static FileWriter WRLOGFILE;
	public static BufferedWriter BWRLOGFILE;
//	private static final long BENCH_START_TIME;
	
	private static Map<RecordKey, Integer> locationTable;
	private static enum PickingMethods { NO, FIFO, LRU, CLOCK };
	private static final PickingMethods PICKING_METHOD = PickingMethods.FIFO;
	public static final int LOC_TABLE_MAX_SIZE;
	// TODO: Maybe we could limit the size of the queue by 2 x LOC_TABLE_MAX_SIZE
	private static Queue<RecordKey> fifoQueue = new LinkedList<RecordKey>();

	static {

//		LOGDIR = new File(".");
//		LOGFILE = new File(LOGDIR, "loc_log.txt");
//		try {
//			WRLOGFILE = new FileWriter(LOGFILE);
//			BWRLOGFILE = new BufferedWriter(WRLOGFILE);
//		} catch (IOException e1) {
//			e1.printStackTrace();
//		}

//		BENCH_START_TIME = System.currentTimeMillis();
		NUM_PARTITIONS = ElasqlProperties.getLoader()
				.getPropertyAsInteger(PartitionMetaMgr.class.getName() + ".NUM_PARTITIONS", 1);
		LOC_TABLE_MAX_SIZE = ElasqlProperties.getLoader()
				.getPropertyAsInteger(PartitionMetaMgr.class.getName() + ".LOC_TABLE_MAX_SIZE", -1);
		if (LOC_TABLE_MAX_SIZE == -1)
			locationTable = new HashMap<RecordKey, Integer>();
		else
			locationTable = new HashMap<RecordKey, Integer>(LOC_TABLE_MAX_SIZE + 1000);
		
//		new PeriodicalJob(3000, 500000, new Runnable() {
//			@Override
//			public void run() {
//				System.out.println("Location Table : " + locationTable.size() +
//						", Queue: " + fifoQueue.size());
//				}
//			}
//		).start();
		
//		new PeriodicalJob(3000, 500000, new Runnable() {
//			@Override
//			public void run() {
//				int[] counts = new int[20];
//				counts[0] = 100_000;
//				
//				for (Entry<RecordKey, Integer> entry : locationTable.entrySet()) {
//					int id = Integer.parseInt((String) entry.getKey().getKeyVal("ycsb_id").asJavaVal());
//					int partId = entry.getValue();
//					
//					if (id <= 100_000) {
//						counts[0]--;
//						counts[partId]++;
//					}
//				}
//				
//				System.out.println(Arrays.toString(counts));
//			}
//		}).start();
		
//		Thread thread = new Thread(new Runnable() {
//			public void run() {
//				try {
//					Thread.sleep(450000);
//					File dir = new File(".");
//					File outputFile = new File(dir, "loc_tbl.txt");
//					FileWriter wrFile = new FileWriter(outputFile);
//					BufferedWriter bwrFile = new BufferedWriter(wrFile);
//					HashMap<RecordKey, LinkedList<Integer>> tmp = (HashMap<RecordKey, LinkedList<Integer>>) locationTable
//							.clone();
//
//					Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
//					RecordKey key;
//					int[] l;
//					int p, iid;
//					for (int j = 0; j < PartitionMetaMgr.NUM_PARTITIONS; j++) {
//						l = new int[PartitionMetaMgr.NUM_PARTITIONS];
//						System.out.print(String.format("Items : %7d ~ %7d -> ", j * 100000, (j + 1) * 100000));
//						for (int i = 1; i <= 100000; i++) {
//							iid = 100000 * j + i;
//							keyEntryMap.put("i_id", new IntegerConstant(iid));
//							key = new RecordKey("item", keyEntryMap);
//							LinkedList<Integer> t = tmp.get(key);
//
//							if (t != null)
//								p = t.getFirst();
//							else
//								p = getRangeLoc(key);
//							l[p]++;
//						}
//						for (int i = 0; i < PartitionMetaMgr.NUM_PARTITIONS; i++)
//							System.out.print(String.format("P %d : %6d ", i, l[i]));
//						System.out.println("");
//					}
//
//					System.out.println("Before");
//					for (int j = 0; j < PartitionMetaMgr.NUM_PARTITIONS; j++) {
//						l = new int[PartitionMetaMgr.NUM_PARTITIONS];
//						System.out.print(String.format("Items : %7d ~ %7d -> ", j * 100000, (j + 1) * 100000));
//						for (int i = 1; i <= 100000; i++) {
//							iid = 100000 * j + i;
//							keyEntryMap.put("i_id", new IntegerConstant(iid));
//							key = new RecordKey("item", keyEntryMap);
//
//							p = key.hashCode() % PartitionMetaMgr.NUM_PARTITIONS;
//							l[p]++;
//						}
//						for (int i = 0; i < PartitionMetaMgr.NUM_PARTITIONS; i++)
//							System.out.print(String.format("P %d : %6d ", i, l[i]));
//						System.out.println("");
//					}
//
//					for (Entry<RecordKey, LinkedList<Integer>> e : tmp.entrySet())
//						bwrFile.write(e.getKey() + " loc: " + e.getValue() + "\n");
//					bwrFile.close();
//				} catch (InterruptedException e) {
//					e.printStackTrace();
//				} catch (IOException e) {
//					e.printStackTrace();
//				}
//			}
//		});
//		thread.start();
	}

	/**
	 * Check if a record is fully replicated on each node.
	 * 
	 * @param key
	 *            the key of the record
	 * @return if the record is fully replicated
	 */
	public abstract boolean isFullyReplicated(RecordKey key);

	/**
	 * Decides the partition of each record.
	 * 
	 * @param key
	 *            the key of the record
	 * @return the id of the partition where the record is
	 */
	public int getCurrentLocation(RecordKey key) {
		Integer old = locationTable.get(key);
		if (old == null)
			return getPartition(key);
		else
			return old;
	}

	public void setCurrentLocation(RecordKey key, int loc) {
//		try {
//			BWRLOGFILE.write((System.currentTimeMillis() - BENCH_START_TIME) + "," + key.getKeyVal("i_id") + "," + loc);
//			BWRLOGFILE.newLine();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
		
		// If the new location matches the original partition, remove it from location table.
		boolean isInLocTable = locationTable.containsKey(key);
		if (isInLocTable && getPartition(key) == loc)
			locationTable.remove(key);
		else {
			if (LOC_TABLE_MAX_SIZE != -1) {
				if (PICKING_METHOD == PickingMethods.FIFO) {
					if (!isInLocTable) {
						fifoQueue.add(key);
					}
				}
			}
			
			locationTable.put(key, new Integer(loc));
		}
	}
	
	/**
	 * Choose the keys that should be removed next time.
	 * 
	 * @return
	 */
	public Set<RecordKey> chooseOverflowedKeys() {
		Set<RecordKey> removedKeys = new HashSet<RecordKey>();
		
		// If the limit is -1 (unlimited), return immediately.
		if (LOC_TABLE_MAX_SIZE == -1)
			return removedKeys;
		
		// Pick the keys that will be removed
		if (PICKING_METHOD == PickingMethods.FIFO) {
			while (locationTable.size() - removedKeys.size() > LOC_TABLE_MAX_SIZE) {
				RecordKey key = fifoQueue.remove();
				if (locationTable.containsKey(key))
					removedKeys.add(key);
			}
		}
		
		return removedKeys;
	}
	
	/**
	 * Get the original location (may not be the current location)
	 * 
	 * @param key
	 * @return
	 */
	public abstract int getPartition(RecordKey key);
}
