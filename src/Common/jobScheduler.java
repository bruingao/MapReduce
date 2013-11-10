package Common;

import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

public final class jobScheduler {
	
	private jobScheduler(){}
	
	/* which job is running on this node */
	ConcurrentHashMap<String, HashSet<Integer>> nodeToJobs 
		= new ConcurrentHashMap<String, HashSet<Integer>>();
	
	/* the job's input file name */
	ConcurrentHashMap<Integer, String> jobToFile
		= new ConcurrentHashMap<Integer, String>();
	
	/* uncompleted mappers */
	ConcurrentHashMap<Integer, Integer> uncompletedMappers
		= new ConcurrentHashMap<Integer, Integer>();
	
	/* job To mapper taskTrackers(Machine's address) and corresponding chunks */
	ConcurrentHashMap<Integer, HashMap<String, HashSet<Integer>>> jobToMappers
		= new ConcurrentHashMap<Integer, HashMap<String, HashSet<Integer>>>();
	
	/* uncompleted reducers */
	ConcurrentHashMap<Integer, Integer> uncompletedReducers
		= new ConcurrentHashMap<Integer, Integer>();
	
	/* job To reducer taskTrackers and corresponding partitions */
	ConcurrentHashMap<Integer, HashMap<String, HashSet<Integer>>> jobToReducers
	= new ConcurrentHashMap<Integer, HashMap<String, HashSet<Integer>>>();
}
