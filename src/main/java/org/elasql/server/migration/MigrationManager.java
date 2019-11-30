package org.elasql.server.migration;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.remote.groupcomm.TupleSet;
import org.elasql.remote.groupcomm.server.ConnectionMgr;
import org.elasql.server.Elasql;
import org.elasql.server.migration.clay.ClayPlanner;
import org.elasql.server.migration.heatgraph.HeatGraph;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.util.ElasqlProperties;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.server.task.Task;

public abstract class MigrationManager {
	private static Logger logger = Logger.getLogger(MigrationManager.class.getName());

	public static final boolean ENABLE_NODE_SCALING;
	public static final boolean IS_SCALING_OUT; // only works when 'ENABLE_NODE_SCALING' = true
	public static final boolean USE_PREDEFINED_PLAN; // only works when 'ENABLE_NODE_SCALING' = true

	private static final long START_MIGRATION_TIME; // in ms
	
	// To use with Hermes
	private static final String PROPERTY_PREFIX = "org.elasql.migration.MigrationMgr";
	
	static {
		ENABLE_NODE_SCALING = ElasqlProperties.getLoader()
				.getPropertyAsBoolean(PROPERTY_PREFIX + ".ENABLE_NODE_SCALING", false);
		IS_SCALING_OUT = ElasqlProperties.getLoader()
				.getPropertyAsBoolean(PROPERTY_PREFIX + ".IS_SCALING_OUT", true);
		USE_PREDEFINED_PLAN = ElasqlProperties.getLoader()
				.getPropertyAsBoolean(PROPERTY_PREFIX + ".USE_PREDEFINED_PLAN", false);
		START_MIGRATION_TIME = ElasqlProperties.getLoader()
				.getPropertyAsLong(PROPERTY_PREFIX + ".START_MIGRATION_TIME", -1);
	}
	
	private static AtomicBoolean isScaled = new AtomicBoolean(false);
	
	public static final int MONITORING_TIME = PartitionMetaMgr.USE_SCHISM? 
			300 * 1000: 5 * 1000;
//			2100 * 1000: 30 * 1000; // for simple workloads
//			30 * 1000: 10 * 1000; // for consolidation

	// Sink ids for sequencers to identify the messages of migration
	public static final int SINK_ID_START_MIGRATION = -555;
	public static final int SINK_ID_ANALYSIS = -777;
	public static final int SINK_ID_ASYNC_PUSHING = -888;
	public static final int SINK_ID_STOP_MIGRATION = -999;
	public static final int SINK_ID_NEXT_MIGRATION = -1111;

	public static long startTime = System.currentTimeMillis();

	private AtomicBoolean isMigrating = new AtomicBoolean(false);
	private AtomicBoolean isMigrated = new AtomicBoolean(false);

	// For recording the migrated status in the other node and the destination
	// node
	private Set<RecordKey> migratedKeys = new HashSet<RecordKey>(1000000);
	// These two sets are created for the source node identifying the migrated
	// records
	// and the parameters of background pushes.
	private Map<RecordKey, Boolean> newInsertedData = new HashMap<RecordKey, Boolean>(1000000);
	private Map<RecordKey, Boolean> analyzedData;

	private AtomicBoolean analysisStarted = new AtomicBoolean(false);
	private AtomicBoolean analysisCompleted = new AtomicBoolean(false);

	// Async pushing
	private static int PUSHING_COUNT = 1000;
	private static final int PUSHING_BYTE_COUNT = 4000000;
	private ConcurrentLinkedQueue<RecordKey> skipRequestQueue = new ConcurrentLinkedQueue<RecordKey>();
	private Map<String, Set<RecordKey>> bgPushCandidates = new HashMap<String, Set<RecordKey>>();
	private boolean roundrobin = true;
	private boolean useCount = true;
	private boolean backPushStarted = false;
	//private boolean isSourceNode;

	// Migration
	private int sourceNode, destNode;
	private boolean isSeqNode;
	
	// XXX: this is not thread-safe. If a clay did not end before next clay started,
	// there would be multiple threads accessing it.
//	private static HashMap<Integer, Vertex> vertexKeys = new HashMap<Integer, Vertex>(1000000);
	// XXX: Note that the "ranges" are "vertices"
	
	// We use a RecordKey to represent a group. 
	protected HashSet<RecordKey> migratingGroups = new HashSet<RecordKey>();
//	public boolean isReachTarget = true;

	public static long MONITOR_STOP_TIME = -1;
	
	// Current States
	private static AtomicBoolean isMonitoring = new AtomicBoolean(false);
	private static AtomicBoolean isClayOperating = new AtomicBoolean(false);
	private static AtomicBoolean isColdMigrated = new AtomicBoolean(false); // for scaling-out
	private static AtomicBoolean isConsolidated = new AtomicBoolean(false);
	
	private ClayPlanner clayPlanner;
	private WorkloadMonitor workloadMonitor;
	private List<MigrationPlan> queuedMigrations;

	public MigrationManager(long printStatusPeriod, int nodeId) {
		this.sourceNode = 0;
		this.destNode = 1;
		this.isSeqNode = (nodeId == ConnectionMgr.SEQ_NODE_ID);
		this.workloadMonitor = new WorkloadMonitor(this);
	}
	
	public void scheduleControllerThread() {
		VanillaDb.taskMgr().runTask(new Task() {
			@Override
			public void run() {
				try {
					if (START_MIGRATION_TIME != -1)
						Thread.sleep(START_MIGRATION_TIME);
					else
						Thread.sleep(getWaitingTime());
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				
				long startTime = System.currentTimeMillis();
				
				if (ENABLE_NODE_SCALING) {
					sendLaunchClayReq(null);
				} else {
					while((System.currentTimeMillis() - startTime) < getMigrationStopTime()) {
						if (!isClayOperating.get())
							sendLaunchClayReq(null);
						else
							if (logger.isLoggable(Level.WARNING))
								logger.warning("Clay is still operating. Stop initialization of next run.");
						try {
							Thread.sleep(getMigrationPreiod());
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}
			}
		});
	}
	
	public static int currentNumOfPartitions() {
		if (ENABLE_NODE_SCALING) {
			if (isScaled.get()) {
				if (IS_SCALING_OUT)
					return PartitionMetaMgr.NUM_PARTITIONS;
				else
					return PartitionMetaMgr.NUM_PARTITIONS - 1;
			} else {
				if (IS_SCALING_OUT)
					return PartitionMetaMgr.NUM_PARTITIONS - 1;
				else
					return PartitionMetaMgr.NUM_PARTITIONS;
			}
		} else
			return PartitionMetaMgr.NUM_PARTITIONS;
	}
	
	public boolean isSourceNode(){
		return (Elasql.serverId() == getSourcePartition());
	}
	
	// Caller: scheduler thread on the sequencer node
	public void startMonitoring() {
		if (logger.isLoggable(Level.INFO)) {
			logger.info("migration manager starts monitoring at " + 
					(System.currentTimeMillis() - startTime) / 1000);
		}
		
		// TODO: We should not need this anymore. Check if we can remove this code.
//		if (PartitionMetaMgr.USE_SCHISM) {
//			// TODO: Schism
//		} else {
//			workloadMonitor = new WorkloadMonitor(this);
//		}
		isMonitoring.set(true);
		isClayOperating.set(true);
		MONITOR_STOP_TIME = System.currentTimeMillis() + MONITORING_TIME;
	}
	
	// Caller: scheduler thread on the sequencer node
	public void analyzeTransactionRequest(Collection<RecordKey> keys) {
		workloadMonitor.recordATransaction(keys);
		
		if (System.currentTimeMillis() > MONITOR_STOP_TIME ||
				(ENABLE_NODE_SCALING && USE_PREDEFINED_PLAN)) {
			stopMonitoring();
		}
	}
	
	private void stopMonitoring() {
		if (logger.isLoggable(Level.INFO))
			logger.info("migration manager stops monitoring at " + (System.currentTimeMillis() - startTime) / 1000);

		isMonitoring.set(false);
		
		if (PartitionMetaMgr.USE_SCHISM) {
			outputMetis(workloadMonitor.retrieveHeatGraphAndReset());
			isClayOperating.set(false);
		} else {
			if (ENABLE_NODE_SCALING)
				isScaled.set(true);
			HeatGraph graph = workloadMonitor.retrieveHeatGraphAndReset();
			
			// Serialize the heat graph as a file for debugging
//			String filename = "heatgraph_" + 
//			((System.currentTimeMillis() - startTime) / 1000) + ".bin";
//			graph.serializeToFile(new File(filename));
			
			clayPlanner = new ClayPlanner(graph);
			generateMigrationPlans();
		}
	}
	
	private void generateMigrationPlans() {
		if (logger.isLoggable(Level.INFO))
			logger.info("Clay starts generating migration plans at " +
					(System.currentTimeMillis() - startTime) / 1000);
		
		VanillaDb.taskMgr().runTask(new Task() {
			@Override
			public void run() {
				if (ENABLE_NODE_SCALING) {
					if (IS_SCALING_OUT) {
						if (USE_PREDEFINED_PLAN && !isColdMigrated.get()) {
							queuedMigrations = generateScalingOutColdMigrationPlans();
						} else
							queuedMigrations = clayPlanner.generateMigrationPlan();
					} else {
						if (USE_PREDEFINED_PLAN && !isColdMigrated.get()) {
							queuedMigrations = generateConsolidationColdMigrationPlans(-1);
						} else if (!USE_PREDEFINED_PLAN && !isConsolidated.get()) {
							int latestLoadPart = clayPlanner.findLeastLoadPartition();
							queuedMigrations = generateConsolidationColdMigrationPlans(latestLoadPart);
						} else 
							queuedMigrations = clayPlanner.generateMigrationPlan();
					}
				} else
					queuedMigrations = clayPlanner.generateMigrationPlan();
				
				if (queuedMigrations != null && !queuedMigrations.isEmpty()) {
					MigrationPlan plan = queuedMigrations.remove(0);
					if (logger.isLoggable(Level.INFO))
						logger.info("Broadcasting the migration plan: " + plan);
					broadcastMigrateKeys(plan.toStoredProcedureRequest());
				} else {
					finishClay();
				}
			}
		});
	}
	
	private void finishClay() {
		if (logger.isLoggable(Level.INFO))
			logger.info("Clay finishes all its jobs at " +
					(System.currentTimeMillis() - startTime) / 1000 + " secs");
		isClayOperating.set(false);
	}
	
	private void triggerNextMigration() {
		if (queuedMigrations != null && !queuedMigrations.isEmpty()) {
			MigrationPlan plan = queuedMigrations.remove(0);
			if (logger.isLoggable(Level.INFO))
				logger.info("Broadcasting the migration plan: " + plan);
			broadcastMigrateKeys(plan.toStoredProcedureRequest());
		} else {
			if (ENABLE_NODE_SCALING) {
				if (IS_SCALING_OUT) {
					if (USE_PREDEFINED_PLAN && !isColdMigrated.get()) {
						isColdMigrated.set(true);
						finishClay();
//						sendLaunchClayReq(null);
						return;
					}
				} else if (!IS_SCALING_OUT) {
					if (USE_PREDEFINED_PLAN && !isColdMigrated.get()) {
						isColdMigrated.set(true);
						finishClay();
//						sendLaunchClayReq(null);
						return;
					} else if (!USE_PREDEFINED_PLAN && !isConsolidated.get()) {
						isConsolidated.set(true);
						finishClay();
						// We need do more Clay to ensure the load is balanced
						sendLaunchClayReq(null);
						return;
					}
				}
			} else {
				finishClay();
			}
			
			// TODO: Check if we can remove this
			//generateMigrationPlans();
		}
	}

	public void addMigrationRepresentKeys(RecordKey[] representatives) {
		// Clear previous migration range
		migratingGroups.clear();
		for (RecordKey key : representatives)
			this.migratingGroups.add(key);
	}
	
	// Sometimes it is hard to put all the keys in Clay.
	// In order to eliminate the need of creating vertices for each key,
	// we merge keys into groups each of which has one representative.
	public abstract RecordKey getRepresentative(RecordKey key);
	
	public boolean keyIsInMigrationRange(RecordKey key) {
		return migratingGroups.contains(getRepresentative(key));
	}
	
	protected abstract List<MigrationPlan> generateScalingOutColdMigrationPlans();
	
	protected abstract List<MigrationPlan> generateConsolidationColdMigrationPlans(int targetPartition);
	
	/**
	 * @return how long it should wait for starting migration experiments (in ms)
	 */
	public abstract long getWaitingTime();
	
	/**
	 * @return how long to initialize a new migration job (in ms)
	 */
	public abstract long getMigrationPreiod();
	
	public abstract long getMigrationStopTime();

	public abstract void sendLaunchClayReq(Object[] metadata);

	public abstract void broadcastMigrateKeys(Object[] metadata);

	public abstract void onReceiveStartMigrationReq(Object[] metadata);

	public abstract void onReceiveAnalysisReq(Object[] metadata);

	public abstract void onReceiveAsyncMigrateReq(Object[] metadata);

	public abstract void onReceiveStopMigrateReq(Object[] metadata);

	public abstract void prepareAnalysis();

	public abstract int recordSize(String tableName);

	public abstract Map<RecordKey, Boolean> generateDataSetForMigration();

	public int getSourcePartition() {
		return sourceNode;
	}

	public void setSourcePartition(int id) {
		this.sourceNode = id;
	}

	public void setDestPartition(int id) {
		this.destNode = id;
	}

	public int getDestPartition() {
		return destNode;
		// return NUM_PARTITIONS - 1;
	}

	// Executed on the source node
	public void analysisComplete(Map<RecordKey, Boolean> analyzedKeys) {
		// Only the source node can call this method
		if (!isSourceNode())
			throw new RuntimeException("Something wrong");

		// Set all keys in the data set as pushing candidates
		// asyncPushingCandidates = new HashSet<RecordKey>(newDataSet.keySet());

		for (RecordKey rk : analyzedKeys.keySet())
			addBgPushKey(rk);

		// Save the set
		analyzedData = analyzedKeys;

		System.out.println("End of analysis: " + (System.currentTimeMillis() - startTime) / 1000);
	}
	
	// For Schism
	public void outputMetis(HeatGraph graph) {
		File metisDir = new File(".");
		metisDir = new File(metisDir, "metis_mesh_" + 
				((System.currentTimeMillis() - startTime) / 1000));
		try {
			graph.generateMetisGraphFile(metisDir);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// NOTE: This can only be called by the scheduler
	public void setRecordMigrated(Collection<RecordKey> keys) {
		if (isSourceNode()) {
			for (RecordKey key : keys) {
				Boolean status = newInsertedData.get(key);
				if (status != null && status == Boolean.FALSE) {
					skipRequestQueue.add(key);
					newInsertedData.put(key, Boolean.TRUE);
				} else {
					status = analyzedData.get(key);
					if (status != null && status == Boolean.FALSE) {
						skipRequestQueue.add(key);
						analyzedData.put(key, Boolean.TRUE);
					}
				}
			}
		} else {
			migratedKeys.addAll(keys);
		}
	}

	public void setRecordLocation(Collection<RecordKey> keys) {
		for (RecordKey key : keys) {
			if (isSourceNode()) {
				Boolean status = newInsertedData.get(key);
				if (status != null && status == Boolean.FALSE) {
					skipRequestQueue.add(key);
					newInsertedData.put(key, Boolean.TRUE);
				} else {
					status = analyzedData.get(key);
					if (status != null && status == Boolean.FALSE) {
						skipRequestQueue.add(key);
						analyzedData.put(key, Boolean.TRUE);
					}
				}
			}
			
			// Update the locations of records
			Elasql.partitionMetaMgr().setPartition(key, destNode);
		}
	}

	// NOTE: This can only be called by the scheduler
	public boolean isRecordMigrated(RecordKey key) {
		if (isSourceNode()) {
			Boolean status = newInsertedData.get(key);
			if (status != null)
				return status;

			status = analyzedData.get(key);
			if (status != null)
				return status;

			// If there is no candidate in the map, it means that the record
			// must not be inserted before the migration starts. Therefore,
			// the record must have been foreground pushed.
			return true;
		} else {
			int partId = Elasql.partitionMetaMgr().getPartition(key);
			if (partId != sourceNode) {
				if (partId != destNode)
					throw new RuntimeException("Something wrong : " + key + " is not in the dest."
							+ " It's in part." + partId + ".");
				return true;
			} else
				return false;

		}
	}

	// NOTE: only for the normal transactions on the source node
	public void addNewInsertKey(RecordKey key) {
		newInsertedData.put(key, Boolean.FALSE);
		addBgPushKey(key);
	}

	// Note that there may be duplicate keys
	private synchronized void addBgPushKey(RecordKey key) {
		Set<RecordKey> set = bgPushCandidates.get(key.getTableName());
		if (set == null)
			set = new HashSet<RecordKey>();
		bgPushCandidates.put(key.getTableName(), set);
		set.add(key);
	}

	public void startAnalysis() {
		analysisStarted.set(true);
		analysisCompleted.set(false);
	}

	public void startMigration() {
		analysisCompleted.set(true);
		isMigrating.set(true);
		isMigrated.set(false);
		if (logger.isLoggable(Level.INFO))
			logger.info("Migration starts at " + (System.currentTimeMillis() - startTime) / 1000);

		// Start background pushes immediately
		startBackgroundPush();
	}

	public void stopMigration() {
		isMigrating.set(false);
		isMigrated.set(true);
		backPushStarted = false;
		if (migratedKeys != null)
			migratedKeys.clear();
		if (newInsertedData != null)
			newInsertedData.clear();
		if (analyzedData != null)
			analyzedData.clear();
		if (logger.isLoggable(Level.INFO))
			logger.info("Migration completes at " +
					(System.currentTimeMillis() - startTime) / 1000 + " secs");
	}
	
	public void onRecvNextMigrationRequest() {
		VanillaDb.taskMgr().runTask(new Task() {
			@Override
			public void run() {
				triggerNextMigration();
			}
		});
	}

	public boolean isAnalyzing() {
		return analysisStarted.get() && !analysisCompleted.get();
	}

	public boolean isMigrating() {
		return isMigrating.get();
	}

	public boolean isMigrated() {
		return isMigrated.get();
	}
	
	public boolean isMonitoring() {
		return isMonitoring.get();
	}

	// Only works on the source node
	protected synchronized RecordKey[] getAsyncPushingParameters() {
		Set<RecordKey> pushingKeys = new HashSet<RecordKey>();
		Set<String> emptyTables = new HashSet<String>();
		int pushing_bytes = 0;

		// YS version
		// for (RecordKey key : asyncPushingCandidates) {
		// seenSet.add(key);
		// if (!isRecordMigrated(key))
		// pushingSet.add(key);
		//
		// if (pushingSet.size() >= PUSHING_COUNT || seenSet.size() >=
		// asyncPushingCandidates.size())
		// break;
		// }
		//
		// // Remove seen records
		// asyncPushingCandidates.removeAll(seenSet);
		//
		// if (logger.isLoggable(Level.INFO))
		// logger.info("The rest size of candidates: " +
		// asyncPushingCandidates.size());

		// Remove the records that have been sent during fore-ground pushing
		RecordKey sentKey = null;
		while ((sentKey = skipRequestQueue.poll()) != null) {
			Set<RecordKey> keys = bgPushCandidates.get(sentKey.getTableName());
			if (keys != null)
				keys.remove(sentKey);
		}

		if (roundrobin) {
			if (useCount) {
				// RR & count
				while (pushingKeys.size() < PUSHING_COUNT && !bgPushCandidates.isEmpty()) {
					for (String tableName : bgPushCandidates.keySet()) {
						Set<RecordKey> set = bgPushCandidates.get(tableName);

						if (set.isEmpty()) {
							emptyTables.add(tableName);
							continue;
						}
						RecordKey key = set.iterator().next();
						pushingKeys.add(key);
						set.remove(key);
					}
					for (String table : emptyTables) {
						bgPushCandidates.remove(table);
					}
				}
			} else {
				// RR & byte
				while (pushing_bytes < PUSHING_BYTE_COUNT && !bgPushCandidates.isEmpty()) {
					for (String tableName : bgPushCandidates.keySet()) {
						Set<RecordKey> set = bgPushCandidates.get(tableName);

						if (set.isEmpty()) {
							emptyTables.add(tableName);
							continue;
						}
						RecordKey key = set.iterator().next();
						pushingKeys.add(key);
						set.remove(key);
						pushing_bytes += recordSize(tableName);
					}
					for (String table : emptyTables) {
						bgPushCandidates.remove(table);
					}
				}
			}
		} else {
			List<String> candidateTables = new LinkedList<String>(bgPushCandidates.keySet());
			// sort by table size , small first
			Collections.sort(candidateTables, new Comparator<String>() {
				@Override
				public int compare(String o1, String o2) {
					int s1 = bgPushCandidates.get(o1).size();
					int s2 = bgPushCandidates.get(o2).size();
					if (s1 > s2)
						return -1;
					if (s1 < s2)
						return 1;
					return 0;
				}
			});
			System.out.println("PUSHING_BYTE_COUNT" + PUSHING_BYTE_COUNT);
			if (useCount) {
				while (!bgPushCandidates.isEmpty() && pushingKeys.size() < PUSHING_COUNT) {
					for (String tableName : candidateTables) {
						Set<RecordKey> set = bgPushCandidates.get(tableName);

						while (!set.isEmpty() && pushingKeys.size() < PUSHING_COUNT) {
							RecordKey key = set.iterator().next();
							pushingKeys.add(key);
							set.remove(key);
						}

						if (set.isEmpty()) {
							emptyTables.add(tableName);
						} else {
							break;
						}
					}
					for (String table : emptyTables) {
						bgPushCandidates.remove(table);
						candidateTables.remove(table);
					}
				}

			} else {
				while (!bgPushCandidates.isEmpty() && pushing_bytes < PUSHING_BYTE_COUNT) {
					for (String tableName : candidateTables) {
						Set<RecordKey> set = bgPushCandidates.get(tableName);

						while (!set.isEmpty() && pushing_bytes < PUSHING_BYTE_COUNT) {
							RecordKey key = set.iterator().next();
							pushingKeys.add(key);
							set.remove(key);
							pushing_bytes += recordSize(tableName);
						}

						System.out.println("listEmpty: " + set.isEmpty());
						System.out.println("pushing_bytes: " + pushing_bytes);

						if (set.isEmpty()) {
							emptyTables.add(tableName);
							// if (!pushingSet.isEmpty()) {
							// VanillaDdDb.taskMgr().runTask(new Task() {
							//
							// @Override
							// public void run() {
							//
							// VanillaDdDb.initAndStartProfiler();
							// try {
							// Thread.sleep(10000);
							// } catch (InterruptedException e) {
							// e.printStackTrace();
							// }
							// VanillaDdDb.stopProfilerAndReport();
							// }
							//
							// });
							// }
							break;
						} else {
							break;
						}
					}
					for (String table : emptyTables) {
						bgPushCandidates.remove(table);
						candidateTables.remove(table);
					}
					System.out.println("pushingSet: " + pushingKeys.size());
					if (!pushingKeys.isEmpty()) {
						break;
					}
				}
			}

		}

		/*
		 * StringBuilder sb = new StringBuilder(); for (String tableName :
		 * roundRobinAsyncPushingCandidates.keySet()) { sb.append(tableName);
		 * sb.append(": ");
		 * sb.append(roundRobinAsyncPushingCandidates.get(tableName).size());
		 * sb.append("\n"); } for (List<RecordKey> table :
		 * roundRobinAsyncPushingCandidates.values()) { candidatesLeft +=
		 * table.size(); }
		 * 
		 * if (logger.isLoggable(Level.INFO)) logger.info(
		 * "The rest size of candidates: " + candidatesLeft + "\n" +
		 * sb.toString());
		 */

		return pushingKeys.toArray(new RecordKey[0]);
	}

	private void startBackgroundPush() {
		if (backPushStarted)
			return;

		backPushStarted = true;

		// Only the source node can send the bg push request
		if (isSourceNode()) {
			// Use another thread to start background pushing
			VanillaDb.taskMgr().runTask(new Task() {
				@Override
				public void run() {
					TupleSet ts = new TupleSet(MigrationManager.SINK_ID_ASYNC_PUSHING);

					Elasql.connectionMgr().pushTupleSet(Elasql.migrationMgr().getSourcePartition(), ts);

					if (logger.isLoggable(Level.INFO))
						logger.info("Trigger background pushing");
					/*
					 * long tCount = (System.currentTimeMillis() -
					 * CalvinStoredProcedureTask.txStartTime) /
					 * printStatusPeriod; StringBuilder sb = new
					 * StringBuilder(); String preStr = "";
					 * 
					 * for (String tableName : bgPushCandidates.keySet()) {
					 * Set<RecordKey> keys = bgPushCandidates.get(tableName); if
					 * (keys != null) { sb.append(tableName); sb.append(":");
					 * sb.append(keys.size()); sb.append(","); } } preStr =
					 * "\nTable remain|" + sb.toString() + "\n"; sb = new
					 * StringBuilder(); for (long i = 0; i < tCount - 1; i++)
					 * sb.append(preStr); if (logger.isLoggable(Level.INFO))
					 * logger.info(sb.toString()); while (true) { sb = new
					 * StringBuilder(); for (String tableName :
					 * bgPushCandidates.keySet()) { Set<RecordKey> keys =
					 * bgPushCandidates.get(tableName); if (keys != null) {
					 * sb.append(tableName); sb.append(":");
					 * sb.append(keys.size()); sb.append(","); } }
					 * 
					 * if (logger.isLoggable(Level.INFO)) logger.info(
					 * "\nTable remain|" + sb.toString()); try {
					 * Thread.sleep(printStatusPeriod); } catch
					 * (InterruptedException e) { e.printStackTrace(); } }
					 */
				}
			});
		}
	}

}