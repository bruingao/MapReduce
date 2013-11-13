package mapred;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import mapred.JobTracker.JOB_RESULT;

import Common.Pair;
import Common.Util;

public class JobClient {
	private static JobConf jobconf;
	
	private static Registry registry;
	
	private static String confPath = "conf/mapred.conf";
		
	/* job tracker's host address */
	private static String jobHostname;
	
	/* job tracker's port number */
	private static Integer jobPort;
	
	/* task tracker's port number */
	private static Integer taskPort;
	
	/* job trakcer's service name */
	private static String jobServiceName;
	
	private static Integer nameRegPort;
	private static Integer dataRegPort;
	private static Integer jobRegPort;
	private static Integer taskRegPort;
	
	/* job id */
	private static int jid;
	
	private void setConf(JobConf conf) {
		jobconf = conf;
	}
	
	public void runJob(JobConf conf) throws RemoteException{
		/* read configuration file */
		JobClient jobclient = new JobClient();
		jobclient.setConf(conf);
		
		Util.readConfigurationFile(confPath, jobclient);
		
		/* submit job to job tacker (submitJob) */
		
		JobTrackerI jobtracker = null;
		
		try {
			registry = LocateRegistry.getRegistry(jobHostname, jobRegPort);
			jobtracker = (JobTrackerI)registry.lookup(jobServiceName);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Exception happend when looking for services of JobTracker!");
			System.exit(-1);
		}
		
		Pair mapper = new Pair(conf.getMapperClass().getName(), 
				Util.readFromFile(conf.getMapperClass().getName()+".class"));
		Pair reducer = new Pair(conf.getReducerClass().getName(),
				Util.readFromFile(conf.getReducerClass().getName()+ ".class"));
		String res = jobtracker.submitJob(conf, mapper, reducer);
		
		/* return the job id or some errors may occur */
		if (res == "OUTEXISTS") {
			System.out.println("output file name is taken!");
			System.exit(-1);
		} else if (res == "INPUTNOTFOUND") {
			System.out.println("input file not found!");
			System.exit(-1);
		} else if (res == "FAIL") {
			System.out.println("Job failed!");
			System.exit(-1);
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
				e.printStackTrace();
				System.out.println("Exception happened when monitoring!");
			}
		}
		
	}
}
