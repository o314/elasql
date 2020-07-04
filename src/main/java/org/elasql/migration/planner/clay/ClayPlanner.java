package org.elasql.migration.planner.clay;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.migration.MigrationPlan;
import org.elasql.migration.planner.MigrationPlanner;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.util.ElasqlProperties;

/**
 * An implementation of Clay [1]. <br />
 * <br />
 * [1] Serafini, Marco, et al.
 * "Clay: fine-grained adaptive partitioning for general database schemas."
 * Proceedings of the VLDB Endowment 10.4 (2016): 445-456.
 * 
 * @author yslin
 *
 */
public class ClayPlanner implements MigrationPlanner {
	private static Logger logger = Logger.getLogger(ClayPlanner.class.getName());
	
	private static final double MULTI_PARTS_COST;
	private static final double OVERLOAD_PERCENTAGE;
	private static final int LOOK_AHEAD_MAX;
	private static final int CLUMP_MAX_SIZE;
	private static final int MAX_CLUMPS;
	
	static {
		MULTI_PARTS_COST = ElasqlProperties.getLoader()
				.getPropertyAsDouble(ClayPlanner.class.getName() + ".MULTI_PARTS_COST", 1);
		OVERLOAD_PERCENTAGE = ElasqlProperties.getLoader()
				.getPropertyAsDouble(ClayPlanner.class.getName() + ".OVERLOAD_PERCENTAGE", 1.3);
		LOOK_AHEAD_MAX = ElasqlProperties.getLoader()
				.getPropertyAsInteger(ClayPlanner.class.getName() + ".LOOK_AHEAD_MAX", 5);
		CLUMP_MAX_SIZE = ElasqlProperties.getLoader()
				.getPropertyAsInteger(ClayPlanner.class.getName() + ".CLUMP_MAX_SIZE", 20);
		MAX_CLUMPS = ElasqlProperties.getLoader()
				.getPropertyAsInteger(ClayPlanner.class.getName() + ".MAX_CLUMPS", 5000);
	}
	
	private HeatGraph heatGraph = new HeatGraph();

	@Override
	public void monitorTransaction(Set<RecordKey> reads, Set<RecordKey> writes) {
		Set<RecordKey> accessedKeys = new HashSet<RecordKey>();
		accessedKeys.addAll(reads);
		accessedKeys.addAll(writes);
		
		for (RecordKey k : accessedKeys) {
			if (Elasql.partitionMetaMgr().isFullyReplicated(k))
				throw new RuntimeException("Given read/write-set contain fully replicated keys");
			
			int partId = Elasql.partitionMetaMgr().getPartition(k);
			heatGraph.updateWeightOnVertex(k, partId);
		}
		heatGraph.updateWeightOnEdges(accessedKeys);
	}

	@Override
	public MigrationPlan generateMigrationPlan() {
		long startTime = System.currentTimeMillis();
		ScatterRangeMigrationPlan plan = new ScatterRangeMigrationPlan();
		int totalPartitions = PartitionMetaMgr.NUM_PARTITIONS;
		int numOfClumpsGenerated = 0;
		
		while (true) {
			List<Partition> partitions = heatGraph.splitToPartitions(totalPartitions, MULTI_PARTS_COST);
			double overloadThreasdhold = calculateOverloadThreasdhold(partitions);
			
			// Debug
			if (logger.isLoggable(Level.FINE)) {
				logger.fine(printPartitionLoading(partitions));
				logger.fine("Threasdhold: " + overloadThreasdhold);
			}
			
			// Find an overloaded partition
			Partition overloadedPart = null;
			for (Partition p : partitions) {
				if (p.getTotalLoad() > overloadThreasdhold) {
					overloadedPart = p;
					break;
				}
			}
			
			if (overloadedPart == null)
				break;
			
			// Generate a clump
			Clump clump = generateClump(overloadedPart, partitions, overloadThreasdhold);
			
			if (clump == null)
				break;
			
			// Some cases will come out a clump that does not have to be migrated.
			// Since it is hard to come out other plan when this case happens,
			// we will stop right here.
			if (!clump.needMigration())
				break;

			// Debug
			if (logger.isLoggable(Level.FINE)) {
				logger.fine("Clump #" + numOfClumpsGenerated + ": " + printClump(clump));
			}
			numOfClumpsGenerated++;
			
			// Generate migration plans from the clump
			ScatterRangeMigrationPlan clumpPlan = clump.toMigrationPlan();
			plan.merge(clumpPlan);
			
			updateMigratedVertices(clump);
			
			if (numOfClumpsGenerated == MAX_CLUMPS) {
				System.out.println("Reach the limit of the number of clumps");
				break;
			}
		}
		
		if (plan.isEmpty())
			return null;
		
		if (logger.isLoggable(Level.INFO)) {
			logger.info(String.format("Clay takes %d ms to generate %d clumps",
					(System.currentTimeMillis() - startTime), numOfClumpsGenerated));
			logger.info("Generated a migration plans: " + plan.countKeys());
		}
		
		return plan;
	}

	@Override
	public void reset() {
		heatGraph = new HeatGraph();
	}
	
	private double calculateOverloadThreasdhold(List<Partition> partitions) {
		double avgLoad = 0.0;
		for (Partition p : partitions)
			avgLoad += p.getTotalLoad();
		avgLoad = avgLoad / partitions.size();
		return avgLoad * OVERLOAD_PERCENTAGE;
	}
	
	private String printPartitionLoading(List<Partition> partitions) {
		StringBuilder sb = new StringBuilder();
		for (Partition p : partitions) {
			sb.append(String.format("Partition %d: local %f, cross %f, total %f\n", 
					p.getPartId(), p.getLocalLoad(), p.getCrossPartLoad(), p.getTotalLoad()));
		}
		return sb.toString();
	}
	
	/**
	 * Algorithm 1 on Clay's paper.
	 */
	private Clump generateClump(Partition overloadedPart,
			List<Partition> partitions, double overloadThreasdhold) {
		Clump currentClump = null, candidateClump = null;
		Partition destPart = null;
		int lookAhead = LOOK_AHEAD_MAX;
		Vertex addedVertex = null;
		
		while (true) {
			if (currentClump == null) {
				addedVertex = overloadedPart.getHotestVertex();
				currentClump = new Clump(addedVertex);
				destPart = findInitialDest(addedVertex, partitions);
				currentClump.setDestination(destPart.getPartId());
				
//				System.out.println("Init clump: " + printClump(currentClump));
			} else {
				if (!currentClump.hasNeighbor())
					return candidateClump;
				
				// Expand the clump
				RecordKey hotKey = currentClump.getHotestNeighbor();
				addedVertex = heatGraph.getVertex(hotKey);
				currentClump.expand(addedVertex);
				
				destPart = updateDestination(currentClump, destPart, partitions, overloadThreasdhold);
				currentClump.setDestination(destPart.getPartId());
				
//				System.out.println("Expanded clump: " + printClump(currentClump));
//				System.out.println("Expanded clump size: " + currentClump.size());
//				System.out.println(String.format("Delta for recv part %d: %f", destPart.getPartId(),
//						calcRecvLoadDelta(currentClump, destPart.getPartId())));
//				System.out.println(String.format("Is feasible ? %s", isFeasible(currentClump, destPart)));
//				System.out.println(String.format("Delta for sender part %d: %f", addedVertex.getPartId(),
//						calcSendLoadDelta(currentClump, addedVertex.getPartId())));
			}
			
			// Examine the clump
			if (isFeasible(currentClump, destPart, overloadThreasdhold)) {
				candidateClump = new Clump(currentClump);
			} else if (candidateClump != null) {
				lookAhead--;
			}
			
			// Limit the size of the clump
			if (currentClump.size() > CLUMP_MAX_SIZE) {
				if (candidateClump != null)
					return candidateClump;
				else
					return currentClump;
			}
			
			if (lookAhead == 0)
				return candidateClump;
		}
	}
	
	private Partition findInitialDest(Vertex v, List<Partition> partitions) {
		int destId = -1;
		
		// Find the most co-accessed partition
		int[] coaccessed = new int[partitions.size()];
		for (OutEdge e : v.getOutEdges().values())
			coaccessed[e.getOpposite().getPartId()]++;
		for (int part = 0; part < coaccessed.length; part++) {
			if (coaccessed[part] > 0) {
				if (destId == -1 || coaccessed[part] > coaccessed[destId]) {
					destId = part;
				}
			}
		}
		
		// There is no co-accessed partition
		if (destId == -1) {
			// Find the least load partition
			double minLoad = Integer.MAX_VALUE;
			for (Partition p : partitions)
				if (p.getPartId() != v.getPartId()) {
					if (destId == -1 || p.getTotalLoad() < minLoad) {
						destId = p.getPartId();
						minLoad = p.getTotalLoad();
					}
				}
		}
		
		return partitions.get(destId);
	}
	
	/**
	 * The formula in Section 7.1 of Clay's paper.
	 */
	private boolean isFeasible(Clump clump, Partition dest, double overloadThreasdhold) {
		double delta = clump.calcRecvLoadDelta(dest.getPartId(), MULTI_PARTS_COST);
		return delta <= 0.0 || (dest.getTotalLoad() + delta < overloadThreasdhold);
	}
	
	/**
	 * Algorithm 2 on Clay's paper.
	 */
	private Partition updateDestination(Clump clump, Partition currentDest,
			List<Partition> partitions, double overloadThreasdhold) {
		if (!isFeasible(clump, currentDest, overloadThreasdhold)) {
			int mostCoaccessedPart = clump.getMostCoaccessedPartition(partitions.size());
			if (mostCoaccessedPart != currentDest.getPartId() &&
					isFeasible(clump, partitions.get(mostCoaccessedPart), overloadThreasdhold))
				return partitions.get(mostCoaccessedPart);
			
			Partition leastLoadPart = getLeastLoadPartition(partitions);
			if (leastLoadPart.getPartId() != currentDest.getPartId() &&
				clump.calcRecvLoadDelta(mostCoaccessedPart, MULTI_PARTS_COST) <
				clump.calcRecvLoadDelta(leastLoadPart.getPartId(), MULTI_PARTS_COST) &&
				isFeasible(clump, leastLoadPart, overloadThreasdhold))
				return leastLoadPart;
		}
		
		return currentDest;
	}
	
	private Partition getLeastLoadPartition(List<Partition> partitions) {
		Partition minLoadPart = partitions.get(0);
		
		for (int partId = 1; partId < partitions.size(); partId++) {
			if (partitions.get(partId).getTotalLoad() < minLoadPart.getTotalLoad()) {
				minLoadPart = partitions.get(partId);
			}
		}
		
		return minLoadPart;
	}
	
	private String printClump(Clump clump) {
		StringBuilder sb = new StringBuilder("[");
		int count = 0;
		for (Vertex v : clump.getVertices()) {
			sb.append(String.format("%s (%d, %f, %f), ", v.getKey(), v.getPartId(),
					v.getVertexWeight(), v.getEdgeWeight()));
			count++;
			if (count >= 5)
				break;
		}
		sb.append(String.format("size: %d] ", clump.getVertices().size()));
		sb.append(String.format("to part.%d.", clump.getDestination()));
		
		return sb.toString();
	}
	
	private void updateMigratedVertices(Clump migratedClump) {
		int destPartId = migratedClump.getDestination();
		for (Vertex v : migratedClump.getVertices())
			v.setPartId(destPartId);
	}
}