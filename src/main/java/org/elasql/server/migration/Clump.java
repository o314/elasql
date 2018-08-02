package org.elasql.server.migration;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.elasql.server.migration.Vertex.OutEdge;
import org.elasql.storage.metadata.PartitionMetaMgr;

public class Clump {

	private class Neighbor implements Comparable<Neighbor> {
		long weight;
		int id;
		int partId;

		public Neighbor(int id, int partId, int weight) {
			this.id = id;
			this.partId = partId;
			this.weight = weight;
		}

		@Override
		public int compareTo(Neighbor other) {
			if (this.weight > other.weight)
				return 1;
			else if (this.weight < other.weight)
				return -1;
			return 0;
		}
		
		@Override
		public String toString() {
			return "<" + id + " on " + partId + " with " + weight +
					" weight>";
		}
	}

	private Map<Integer, Vertex> vertices;
	private Map<Integer, Neighbor> neighbors;
//	private double load;

	public Clump(Vertex initVertex) {
//		this.load = 0;
		this.vertices = new HashMap<Integer, Vertex>();
		this.neighbors = new HashMap<Integer, Neighbor>();
		
		addVertex(initVertex);
	}
	
	public void expand(Vertex neighbor) {
		if (neighbors.remove(neighbor.getId()) == null) {
			throw new RuntimeException("There is no neighbor with id " +
					neighbor.getId() + " in the clump.");
		}
		addVertex(neighbor);
	}
	
	public boolean removeNeighbor(int neighborVertexId) {
		return neighbors.remove(neighborVertexId) != null;
	}

	public int getHotestNeighbor() {
		return Collections.max(neighbors.values()).id;
	}
	
	public boolean hasNeighbor() {
		return !neighbors.isEmpty();
	}
	
	public int size() {
		return vertices.size();
	}
	
	public List<MigrationPlan> toMigrationPlans(int destPart) {
		MigrationPlan[] sources = new MigrationPlan[PartitionMetaMgr.NUM_PARTITIONS];
		for (int i = 0; i < sources.length; i++)
			sources[i] = new MigrationPlan(i, destPart);
		
		// Put the vertices to the plans
		for (Vertex v : vertices.values()) {
			// Mark all vertices as moved
			v.markMoved();
			
			sources[v.getPartId()].addKey(v.getId());
		}
		
		List<MigrationPlan> candidatePlans = new LinkedList<MigrationPlan>();
		
		for (int i = 0; i < sources.length; i++)
			if (sources[i].keyCount() > 0)
				candidatePlans.add(sources[i]);
		
		return candidatePlans;
	}

	@Override
	public String toString() {
//		String str = "Load : " + this.load + " Neighbor : " + neighbors.size() + "\n";
//		str += "Candidate Id : ";
//		ArrayList<Integer> cc = new ArrayList<Integer>(candidateIds);
//		Collections.sort(cc);
//		for (int id : cc)
//			str += ", " + id;
		/*
		 * for (int id : candidateIds) if (id*MigrationManager.dataRange >
		 * 100000) str += "Somethigs Wrong Tuple " + id + "in candidateIds\n";
		 */
//		str += "\n";
//		return str;
		
		StringBuilder sb = new StringBuilder();
		
		sb.append("{vertices: [");
		sb.append(vertices);
		sb.append("], neighbor: [");
		sb.append(neighbors);
		sb.append("]}");

		return sb.toString();
	}

	private void addVertex(Vertex v) {
//		load += v.getVertexWeight();
		vertices.put(v.getId(), v);
		
		// Simplified version: it does not consider 
		// the vertices on other partition nodes.
//		for (OutEdge o : v.getEdge().values())
//			if (o.partId == v.getPartId())
//				addNeighbor(o.id, o.partId, o.weight);
		
		// Correct version: consider the vertices on all partitions
		for (OutEdge o : v.getOutEdges().values())
			addNeighbor(o.vertexId, o.partId, o.weight);
	}

	private void addNeighbor(Integer id, int partId, int weight) {
		// Avoid self loop
		if (vertices.containsKey(id))
			return;

		Neighbor w = neighbors.get(id);
		if (w == null)
			neighbors.put(id, new Neighbor(id, partId, weight));
		else
			w.weight += weight;
	}
}