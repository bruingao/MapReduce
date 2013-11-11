package Common;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import mapred.JobTracker;

public final class jobScheduler {
	
	private jobScheduler(){}
	
//	/* which mapper is running on this node */
//	private static ConcurrentHashMap<String, HashMap<Integer, HashSet<Integer>>> nodeToMappers 
//		= new ConcurrentHashMap<String, HashMap<Integer, HashSet<Integer>>>();
//	
//	/* which job is running on this node */
//	private static ConcurrentHashMap<String, HashMap<Integer,Integer>> nodeToReducers 
//		= new ConcurrentHashMap<String, HashMap<Integer,Integer>>();
	
	/* how many mappers run on this node */
	public static ConcurrentHashMap<String, Integer> nodeToNumMappers
	 	= new ConcurrentHashMap<String, Integer>();
	
	/* how many reducers run on this node */
	private static ConcurrentHashMap<String, Integer> nodeToNumReducers
		= new ConcurrentHashMap<String, Integer>();
	
	/* node status */
	private static ConcurrentHashMap<String, Boolean> nodeStatus
		= new ConcurrentHashMap<String, Boolean>();
	
	/* number of mappers */
	private static ConcurrentHashMap<Integer, Integer> numMappers
		= new ConcurrentHashMap<Integer, Integer>();
	
	/* uncompleted mappers */
	private static ConcurrentHashMap<Integer, Integer> uncompletedMappers
		= new ConcurrentHashMap<Integer, Integer>();
	
	/* job To mapper taskTrackers(Machine's address) and corresponding chunks */
	public static ConcurrentHashMap<Integer, HashMap<String, HashSet<Integer>>> jobToMappers
		= new ConcurrentHashMap<Integer, HashMap<String, HashSet<Integer>>>();
	
	/* number of reducers */
	private static ConcurrentHashMap<Integer, Integer> numReducers
		= new ConcurrentHashMap<Integer, Integer>();
	
	/* uncompleted reducers */
	private static ConcurrentHashMap<Integer, Integer> uncompletedReducers
		= new ConcurrentHashMap<Integer, Integer>();
	
	/* job To reducer taskTrackers and corresponding partitions */
	private static ConcurrentHashMap<Integer, HashMap<String, HashSet<Integer>>> jobToReducers
	= new ConcurrentHashMap<Integer, HashMap<String, HashSet<Integer>>>();
	
	public static double getMapperPercent(int jid) {
		return (double)uncompletedMappers.get(jid)/(double)numMappers.get(jid);
	}
	
	public static double getReducerPercet(int jid) {
		if(numReducers.get(jid) == -1)
			return 0;
		return (double)uncompletedReducers.get(jid)/(double)numReducers.get(jid);
	}
	
	public static boolean checkStatus (int jid) {
		return (uncompletedReducers.get(jid) == 0) && (uncompletedMappers.get(jid) == 0);
	}
	
	public static HashMap<String,HashSet<Integer>> decideMappers(HashMap<Integer, HashSet<String>> filechunks, int jid) {
		/* check every filechunk's replication nodes and choose the one with fewest mappers running.
		 */
		
		HashMap<String,HashSet<Integer>> nodeTochunks = new HashMap<String, HashSet<Integer>>();
		
		for (int chunk : filechunks.keySet()) {
			HashSet<String> nodes = filechunks.get(chunk);
			/* choose among those nodes which have the file chunk */
			String opNode = chooseBestNode(nodes);
			if (opNode == null) {
				System.out.println("This cannot happen because this project assumes " +
						"that it is impossible for all the replicas having failed!");
				return null;
			}
			HashSet<Integer> chunks = nodeTochunks.get(opNode);
			if(chunks == null)
				chunks = new HashSet<Integer>();
			
			chunks.add(chunk);
			nodeTochunks.put(opNode, chunks);
			
		}
		
		/* add mappers number to that every chosen node */
		for(String node : nodeTochunks.keySet()) {
			nodeToNumMappers.put(node, nodeToNumMappers.get(node)+1);
		}
		
		/* add corresponding information */
		jobToMappers.put(jid, nodeTochunks);
		numMappers.put(jid, nodeTochunks.size());
		uncompletedMappers.put(jid, nodeTochunks.size());
		
		/* -1 indicates that reducer has not began its work yet */
		numReducers.put(jid, -1);
		
		return nodeTochunks;
	}
	
	
	public static String chooseBestNode(Set<String> set) {
		if(set == null)
			return null;
		String res = null;
		for (String node : set) {
			if (nodeStatus.get(node)) {
				int num = nodeToNumMappers.get(node);
				if (res == null || num < nodeToNumMappers.get(res))
					res = node;
			}
		}
		
		return res;
	}
	
}
