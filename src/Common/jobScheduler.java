package Common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import mapred.JobTracker;
import mapred.jobThread;

public final class jobScheduler {
	
	private jobScheduler(){}
	
//	/* which mapper is running on this node */
//	private static ConcurrentHashMap<String, HashMap<Integer, HashSet<Integer>>> nodeToMappers 
//		= new ConcurrentHashMap<String, HashMap<Integer, HashSet<Integer>>>();
//	
//	/* which job is running on this node */
//	private static ConcurrentHashMap<String, HashMap<Integer,Integer>> nodeToReducers 
//		= new ConcurrentHashMap<String, HashMap<Integer,Integer>>();
	
//	/* how many mappers run on this node */
//	public static ConcurrentHashMap<String, Integer> nodeToNumMappers
//	 	= new ConcurrentHashMap<String, Integer>();
//	
//	/* how many reducers run on this node */
//	public static ConcurrentHashMap<String, Integer> nodeToNumReducers
//		= new ConcurrentHashMap<String, Integer>();
	
	public static ConcurrentHashMap<String, Integer> nodeToNumTasks
 	= new ConcurrentHashMap<String, Integer>();
	
	/* which map jobs are on which node */
	public static ConcurrentHashMap<String, HashSet<Integer>> nodeToMapJobs
		= new ConcurrentHashMap<String, HashSet<Integer>>();
	
	/* which reduce jobs are on which node */
	public static ConcurrentHashMap<String, HashSet<Pair>> nodeToReduceJobs
		= new ConcurrentHashMap<String, HashSet<Pair>>();
	
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
	private static ConcurrentHashMap<Integer, ArrayList<String>> jobToReducers
		= new ConcurrentHashMap<Integer, ArrayList<String>>();
	
//	/* which nodes has the intermediate files */
//	private static ConcurrentHashMap<Integer, String> jobToInter
//		=new ConcurrentHashMap<Integer, String>(); 
	
	public static double getMapperPercent(int jid) {
		return (1 - (double)uncompletedMappers.get(jid)/(double)numMappers.get(jid));
	}
	
	public static double getReducerPercet(int jid) {
		if(numReducers.get(jid) == -1)
			return 0;
		return (1 - (double)uncompletedReducers.get(jid)/(double)numReducers.get(jid));
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
			String opNode = chooseBestNode(nodes, nodeToNumTasks);
			nodeToNumTasks.put(opNode, nodeToNumTasks.get(opNode)-1);
			double point1 = -100;
			
			if(nodes == null) {
				System.out.println("cannot find opNode");
				return null;
			}
			
			if(opNode == null) {
				System.out.println("opNode null");
				return null;
			}
			
			if(nodeToNumTasks.get(opNode)!=null)
				point1 = JobTracker.localBonus 
						+ JobTracker.taskBonus * (JobTracker.maxTasks - nodeToNumTasks.get(opNode));
			
			if (opNode == null) {
				System.out.println("This cannot happen because this project assumes " +
						"that it is impossible for all the replicas having failed!");
				return null;
			}
			
			/* choose another one which has the least number of mappers */
			String opNode2 = chooseBestNode(nodeStatus.keySet(), nodeToNumTasks);
			nodeToNumTasks.put(opNode2, nodeToNumTasks.get(opNode2)-1);

			double point2 = -100;
			if(nodeToNumTasks.get(opNode2)  != null)
				point2 = JobTracker.taskBonus * (JobTracker.maxTasks - nodeToNumTasks.get(opNode2));
			
			/* compare their points and obtain the best one */
			opNode2 = point1 >= point2? opNode:opNode2;
			
			HashMap<Integer, String> chunks = nodeTochunks.get(opNode);
			if(chunks == null)
				chunks = new HashMap<Integer, String>();
			
			nodeToNumTasks.put(opNode2, nodeToNumTasks.get(opNode2)+1);
			chunks.put(chunk, opNode);
			nodeTochunks.put(opNode2, chunks);
			
		}
		
		/* add mappers number to that every chosen node */
		for(String node : nodeTochunks.keySet()) {
//			int mc = JobTracker.minChunk;
//			nodeToNumTasks.put(node, nodeToNumTasks.get(node)+ 1);
			HashSet<Integer> jobs = nodeToMapJobs.get(node);
			
			jobs.add(jid);
			nodeToMapJobs.put(node, jobs);
		}
		
		/* add corresponding information */
		jobToMappers.put(jid, nodeTochunks);
		numMappers.put(jid, nodeTochunks.size());
		uncompletedMappers.put(jid, nodeTochunks.size());
		
		/* -1 indicates that reducer has not began its work yet */
		numReducers.put(jid, -1);
		uncompletedReducers.put(jid, -1);
		
		return nodeTochunks;
	}
	
	
	public static ArrayList<String> decideReducers(int jid, int num) {
		ArrayList<String> nodes = new ArrayList<String>();
		for(int i = 0;i < num; i++) {
			String opNode = chooseBestNode(nodeStatus.keySet(), nodeToNumTasks);
			if(opNode == null) {
//				if(i > 0) {
//					for(int j = 0;j<i;j++) {
//						nodeToNumTasks.put(nodes.get(j), nodeToNumTasks.get(nodes.get(j))-1);
//					}
//				}
				return null;
			}
			nodes.add(opNode);
//			nodeToNumTasks.put(opNode, nodeToNumTasks.get(opNode)+1);
		}
		
		/* update the hashmap independently from the last loop in case of choosebestnode return null */
		if(nodes != null) {
			for (int i = 0; i < num; i++) {
				HashSet<Pair> jobs = nodeToReduceJobs.get(nodes.get(i));
				
				jobs.add(new Pair(jid, i));
				nodeToReduceJobs.put(nodes.get(i), jobs);
			}
		}
		
		jobToReducers.put(jid, nodes);
		numReducers.put(jid, num);
		uncompletedReducers.put(jid, num);
		
		return nodes;
	}
	
	public static void mapperSucceed(int jid, String tnode) {
//		int chunknum = jobToMappers.get(jid).get(tnode).size();
//		
//		int mc = JobTracker.minChunk;
		System.out.println("before succeed: " + nodeToNumTasks.get(tnode));
//		nodeToNumMappers.put(tnode, nodeToNumMappers.get(tnode) -  (chunknum + mc -1)/mc);
		
		nodeToNumTasks.put(tnode, nodeToNumTasks.get(tnode) -  1);
		
		System.out.println("after succeed: " + nodeToNumTasks.get(tnode));

		/* should not remove mappers's information 
		 * in case of jobtracker failure in the process 
		 * of retrieving intermediate data by reducers */
		//jobToMappers.get(jid).get(tnode).clear();
		nodeToMapJobs.get(tnode).remove(jid);
		System.out.println("before succeed: " + uncompletedMappers.get(jid));
		uncompletedMappers.put(jid, uncompletedMappers.get(jid) - 1);
		System.out.println("after succeed: " + uncompletedMappers.get(jid));

	}
	
	public static void reducerSucceed(int jid, String tnode, int partition) {
		
		/* Decrease the number of tnode by 1 */
		nodeToNumTasks.put(tnode, nodeToNumTasks.get(tnode) - 1);
		
		nodeToReduceJobs.get(tnode).remove(new Pair(jid, partition));

		/* Decrease the number of uncompleted reducers by 1 */
		uncompletedReducers.put(jid, uncompletedReducers.get(jid)-1);
	}
	
	public static Pair mapperFail(int jid, String tnode, HashMap<Integer,HashSet<String>> chunks) {
		/* set the failed node status to false */
		nodeStatus.put(tnode, false);
		
		/* now choose the optimal node to handle the job */
		String opNode = chooseBestNode(nodeStatus.keySet(), nodeToNumTasks);
				
		/* what chunk does this failed node has */
		HashMap<Integer, String> failChunk = jobToMappers.get(jid).get(tnode);
		
//		int chunksize = failChunk.size();
//		
//		int mc = JobTracker.minChunk;
		nodeToNumTasks.put(tnode, nodeToNumTasks.get(tnode) - 1);
		
		nodeToMapJobs.get(tnode).remove(jid);

		jobToMappers.get(jid).remove(tnode);
		
		if(opNode ==null)
			return null;
		
		/* if chunks != null, it means this error is caused by datanode failure 
		 * so we need rechoose the datanode */
		if (chunks != null) {
			for(int ck : chunks.keySet()) {
				if(failChunk.keySet().contains(ck)) {
					if(chunks.get(ck)==null)
						continue;
					String newNode = (String) (chunks.get(ck).toArray())[0];
					System.out.println("new node: "+newNode);
					failChunk.put(ck, newNode);
				}
			}
		}
		
		/* what chunk does this optimal node has */
		HashMap<Integer, String> opChunk = jobToMappers.get(jid).get(opNode);
		
		if (opChunk == null)
			opChunk = failChunk;
		else
			opChunk.putAll(failChunk);
				
		jobToMappers.get(jid).put(opNode, opChunk);
		
//		nodeToNumTasks.put(opNode, nodeToNumTasks.get(opNode) + 1);
		
		nodeToMapJobs.get(opNode).add(jid);
		
		/* return the best node and the failed chunk */
		return new Pair(opNode, failChunk);
	}
	
	/* reducer fail */
	public static String reducerFail(int jid, String tnode, int partition) {
		
		nodeToNumTasks.put(tnode, nodeToNumTasks.get(tnode) - 1);
		
		nodeStatus.put(tnode, false);
		
		nodeToReduceJobs.get(tnode).remove(new Pair(jid, partition));
		
		String opNode = chooseBestNode(nodeStatus.keySet(), nodeToNumTasks);
		
		if(opNode == null)
			return null;
		
		nodeToReduceJobs.get(opNode).add(new Pair(jid,partition));
		
		jobToReducers.get(jid).set(partition, opNode);
		
		return opNode;
		
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
		
		if(res != null)
			nodeToNumTasks.put(res, nodeToNumTasks.get(res)+1);
		
		return res;
	}
	
	public static void removeAll(Integer jid) {
		
		for (String node : jobToMappers.get(jid).keySet()) {
			nodeToNumTasks.put(node, nodeToNumTasks.get(node)-1);
		}
		
		for (String node : jobToReducers.get(jid)) {
			nodeToNumTasks.put(node, nodeToNumTasks.get(node)-1);
		}
		
		for (String node : nodeToMapJobs.keySet()) {
			nodeToMapJobs.get(node).remove(jid);
		}
		
		for (String node : nodeToReduceJobs.keySet()) {
			for (Pair pair : nodeToReduceJobs.get(node)) {
				if (jid == (int)pair.name) {
					nodeToReduceJobs.get(node).remove(pair);
				}
			}			
		}
		
		numMappers.remove(jid);

		
		uncompletedMappers.remove(jid);

		
		jobToMappers.remove(jid);

		
		numReducers.remove(jid);

		
		uncompletedReducers.remove(jid);

		
		jobToReducers.remove(jid);
	}
	
}
