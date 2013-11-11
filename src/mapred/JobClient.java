package mapred;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import mapred.JobTracker.JOB_RESULT;

import Common.Util;

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
	
	/* job trakcer's service name */
	private static String jobServiceName;
	
	/* job id */
	private static int jid;
	
	public void setConf(JobConf conf) {
		jobconf = conf;
	}
	
	public static void runJob(JobConf conf) throws RemoteException{
		/* read configuration file */
		JobClient jobclient = new JobClient();
		jobclient.setConf(conf);
		
		Util.readConfigurationFile(confPath, jobclient);
		
		/* submit job to job tacker (submitJob) */
		
		JobTrackerI jobtracker = null;
		
		try {
			registry = LocateRegistry.getRegistry(jobHostname, jobPort);
			jobtracker = (JobTrackerI)registry.lookup(jobHostname+"/"+jobServiceName);
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		String res = jobtracker.submitJob(conf);
		
		/* return the job id or some errors may occur */
		if (res == "OUTEXISTS") {
			System.out.println("output file name is taken!");
		} else if (res == "INPUTNOTFOUND") {
			System.out.println("input file not found!");
		} else if (res == "FAIL") {
			System.out.println("Job failed!");
		}
		
		jid = Integer.parseInt(res);
		
		/* periodically check the status (checkStatus)
		 * if INPROGRESS check the percetage (checkMapper and checkReducer)
		 * such as map ?% and reduce ?% until both 100% or return FAIL */
		while (true) {
			JOB_RESULT status = jobtracker.checkStatus(jid);
			if(status == JOB_RESULT.INPROGRESS) {
				double mp = jobtracker.checkMapper(jid);
				double rp = jobtracker.checkReducer(jid);
				System.out.printf("Mapper: %f; Reducer: %f\n", mp, rp);
			} else if (status == JOB_RESULT.FAIL) {
				System.out.println("job failed!");
				break;
			} else if (status == JOB_RESULT.SUCCESS) {
				System.out.println("job succeed!");
				break;
			}
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
}
