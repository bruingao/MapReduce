package Common;

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
	public static ConcurrentHashMap<String, Integer> nodeToNumReducers
		= new ConcurrentHashMap<String, Integer>();
	
	/* node status */
	public static ConcurrentHashMap<String, Boolean> nodeStatus
		= new ConcurrentHashMap<String, Boolean>();
	
	/* number of mappers */
	private static ConcurrentHashMap<Integer, Integer> numMappers
		= new ConcurrentHashMap<Integer, Integer>();
	
	/* uncompleted mappers */
	private static ConcurrentHashMap<Integer, Integer> uncompletedMappers
		= new ConcurrentHashMap<Integer, Integer>();
	
	/* job To mapper taskTrackers(Machine's address) and corresponding chunks */
	public static ConcurrentHashMap<Integer, HashMap<String, HashMap<Integer, String>>> jobToMappers
		= new ConcurrentHashMap<Integer, HashMap<String, HashMap<Integer, String>>>();
	
	/* number of reducers */
	private static ConcurrentHashMap<Integer, Integer> numReducers
		= new ConcurrentHashMap<Integer, Integer>();
	
	/* uncompleted reducers */
	private static ConcurrentHashMap<Integer, Integer> uncompletedReducers
		= new ConcurrentHashMap<Integer, Integer>();
	
	/* job To reducer taskTrackers and corresponding partitions */
	private static ConcurrentHashMap<Integer, HashMap<String, HashSet<Integer>>> jobToReducers
		= new ConcurrentHashMap<Integer, HashMap<String, HashSet<Integer>>>();
	
//	/* which nodes has the intermediate files */
//	private static ConcurrentHashMap<Integer, String> jobToInter
//		=new ConcurrentHashMap<Integer, String>(); 
	
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
	
	public static HashMap<String,HashMap<Integer, String>> decideMappers(HashMap<Integer, HashSet<String>> filechunks, int jid) {
		/* check every filechunk's replication nodes and choose the one with fewest mappers running.
		 */
		
		HashMap<String,HashMap<Integer, String>> nodeTochunks = new HashMap<String, HashMap<Integer, String>>();
		
		for (int chunk : filechunks.keySet()) {
			HashSet<String> nodes = filechunks.get(chunk);
			/* choose among those nodes which have the file chunk */
			String opNode = chooseBestNode(nodes, nodeToNumMappers);
			
			double point1 = JobTracker.localBonus 
					+ JobTracker.taskBonus * (JobTracker.maxMappers - nodeToNumMappers.get(opNode));
			
			if (opNode == null) {
				System.out.println("This cannot happen because this project assumes " +
						"that it is impossible for all the replicas having failed!");
				return null;
			}
			
			/* choose another one which has the least number of mappers */
			String opNode2 = chooseBestNode(nodeStatus.keySet(), nodeToNumMappers);
			
			double point2 = JobTracker.taskBonus * (JobTracker.maxMappers - nodeToNumMappers.get(opNode2));
			
			/* compare their points and obtain the best one */
			opNode = point1 <= point2? opNode:opNode2;
			
			HashMap<Integer, String> chunks = nodeTochunks.get(opNode);
			if(chunks == null)
				chunks = new HashMap<Integer, String>();
			
			chunks.put(chunk, opNode);
			nodeTochunks.put(opNode, chunks);
			
		}
		
		/* add mappers number to that every chosen node */
		for(String node : nodeTochunks.keySet()) {
			int mc = JobTracker.minChunk;
			nodeToNumMappers.put(node, nodeToNumMappers.get(node)+ (nodeTochunks.get(node).size()+mc-1)/mc);
		}
		
		/* add corresponding information */
		jobToMappers.put(jid, nodeTochunks);
		numMappers.put(jid, nodeTochunks.size());
		uncompletedMappers.put(jid, nodeTochunks.size());
		
		/* -1 indicates that reducer has not began its work yet */
		numReducers.put(jid, -1);
		
		return nodeTochunks;
	}
	
	
	public static void decideReducers(int jid, int num) {
		for(int i = 0;i < num; i++) {
			String opNode = chooseBestNode(nodeStatus.keySet(), nodeToNumReducers);
		}
	}
	
	public static void mapperSucceed(int jid, String tnode) {
		int chunknum = jobToMappers.get(jid).get(tnode).size();
		
		int mc = JobTracker.minChunk;
		nodeToNumMappers.put(tnode, nodeToNumMappers.get(tnode) -  (chunknum + mc -1)/mc);
		
		/* should not remove mappers's information 
		 * in case of jobtracker failure in the process 
		 * of retrieving intermediate data by reducers */
		//jobToMappers.get(jid).get(tnode).clear();
		
		uncompletedMappers.put(jid, uncompletedMappers.get(jid) - 1);
	}
	
	public static Pair mapperFail(int jid, String tnode) {
		/* set the failed node status to false */
		nodeStatus.put(tnode, false);
		
		/* now choose the optimal node to handle the job */
		String opNode = chooseBestNode(nodeStatus.keySet(), nodeToNumMappers);
		
		/* what chunk does this failed node has */
		HashMap<Integer, String> failChunk = jobToMappers.get(jid).get(tnode);
		
		/* what chunk does this optimal node has */
		HashMap<Integer, String> opChunk = jobToMappers.get(jid).get(opNode);
		
		if (opChunk == null)
			opChunk = failChunk;
		else
			opChunk.putAll(failChunk);
		
		jobToMappers.get(jid).remove(tnode);
		
		jobToMappers.get(jid).put(opNode, opChunk);
		
		/* return the best node and the failed chunk */
		return new Pair(opNode, failChunk);
	}
	
	/* choose the best tasktracker whose mapper or reducer with least workload */
	public static String chooseBestNode(Set<String> set, ConcurrentHashMap<String, Integer> workers) {
		if(set == null)
			return null;
		String res = null;
		for (String node : set) {
			if (nodeStatus.get(node)) {
				int num = workers.get(node);
				if (res == null || num < workers.get(res))
					res = node;
			}
		}
		
		return res;
	}
	
}
