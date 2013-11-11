package mapred;

import java.rmi.registry.Registry;

public class JobClient {
	private static JobConf jobconf;
	
	private static Registry registry;
	
	private static String confPath = "src/conf/mapred.conf";
	
	private static String slavePath = "src/conf/slaves";
	
	/* job tracker's host address */
	private static String jobHostname;
	
	/* job tracker's port number */
	private static Integer jobPort;
	
	/* task tracker's port number */
	private static Integer taskPort;
	
	/* registry port number */
	private static Integer registryPort;
	
	public static void setConf(JobConf conf) {
		jobconf = conf;
	}
	
	public static void runJob(JobConf conf){
		/* read configuration file */
		
		/* submit job to job tacker (submitJob) */
		
		/* periodically check the status (checkStatus)
		 * if INPROGRESS check the percetage (checkMapper and checkReducer)
		 * such as map ?% and reduce ?% until both 100% or return FAIL */
	}
}
