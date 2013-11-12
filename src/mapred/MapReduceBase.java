package mapred;

import java.io.UnsupportedEncodingException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashSet;

import dfs.DataNodeI;

import Common.Util;

public class MapReduceBase {
	
	public static void main(String[] args) {
		/* jobid, filepath, localflag, numOfChunks, inputfile, chunks, datanodeHost, regPort, datanode service name */
		int jid = Integer.parseInt(args[0]);
		
		String inputPath = args[1];
		
		boolean local = Boolean.parseBoolean(args[2]);
		
		Integer numOfChunks = Integer.parseInt(args[3]);
		
		String filename = args[4];
		
		HashSet<Integer> chunks = new HashSet<Integer>();
		
		int i = 0;
		while (i < numOfChunks) {
			chunks.add(Integer.parseInt(args[i]));
			i++;
		}
		
		String content[] = new String[numOfChunks];
		
		/* read file according to file locality */
		int c = 0;
		for (int ck : chunks) {
			if (local) {
				try {
					content[c] = new String(Util.readFromFile(inputPath+filename+ck), "UTF-8");
				} catch (UnsupportedEncodingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			else {
				String datanodeHost = args[4 + numOfChunks];
				
				int regPort = Integer.parseInt(args[5 + numOfChunks]);
				
				String service = args[6 + numOfChunks];
				
				try {
					Registry reg = LocateRegistry.getRegistry(datanodeHost, regPort);
					DataNodeI datanode = (DataNodeI)reg.lookup(service);
					content[c] = new String(datanode.read(filename+ck),"UTF-8");
				} catch (RemoteException e) {
					e.printStackTrace();
				} catch (NotBoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (UnsupportedEncodingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
		
	}
}
