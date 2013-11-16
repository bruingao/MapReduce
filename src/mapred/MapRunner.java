package mapred;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import Common.Pair;
import Common.Util;
import Common.Collector;
import Common.Partitioner;

import dfs.DataNodeI;
import format.inputFormatAbs;

public class MapRunner {
	
	private static Mapper mapper;

	public static Mapper getMapper() {
		return mapper;
	}

	public static void setMapper(Mapper mapper) {
		MapRunner.mapper = mapper;
	}
	
	public static void main(String[] args) {
		/* mapper classname, jobid, num of partitions, partitionPath, numOfChunks, inputfile, chunks, datanodeHost, regPort, datanode service name */
		
//		for(int i = 0;i <args.length;i++)
//			System.out.println(args[i]);
		
		String mapperName = args[0];
		
		int numPartitions = Integer.parseInt(args[1]);
		
		String partitionPath = args[2];
		
		int jid = Integer.parseInt(args[3]);				
				
		int numOfChunks = Integer.parseInt(args[4]);
		
		String filename = args[5];
		
		String inputformat = args[6];
		
		ArrayList<Integer> chunks = new ArrayList<Integer>();
		
		int i = 0;
		while (i < numOfChunks) {
			chunks.add(Integer.parseInt(args[7 + i]));
			i++;
		}
		
		
//		System.out.println("phase 1:succeed!");
		
		try {
			Class<Mapper> ma = (Class<Mapper>) Class.forName(mapperName);
		
			Constructor<Mapper> cma = ma.getConstructor();
			
			mapper = cma.newInstance();
			
//			System.out.println("phase 2:succeed!");
			
			String content[] = new String[numOfChunks];
			
			/* read file */
			int c = 0;
			
			Collector collector = new Collector();
			
			int regPort = Integer.parseInt(args[7 + numOfChunks*2]);
			String service = args[8 + numOfChunks * 2];			
			
			for (int ck : chunks) {
				String datanodeHost = args[7 + numOfChunks + c];				
//				System.out.println("datanodeHostname: "+datanodeHost);
//				System.out.println("service name: "+service);
				Registry reg = LocateRegistry.getRegistry(datanodeHost, regPort);
				DataNodeI datanode = (DataNodeI)reg.lookup(service);
				content[c] = new String(datanode.read(filename+"-"+ck),"UTF-8");
				
				/* produce key pair */
				Class<inputFormatAbs> iFormat = (Class<inputFormatAbs>) Class.forName(inputformat);
				Constructor<inputFormatAbs> constuctor = iFormat.getConstructor(String.class);
				
				inputFormatAbs iformat = constuctor.newInstance(content[c]);
				
				List<Pair> pairs = iformat.getkvPairs();
				
				for(Pair pair : pairs) {
					mapper.map(pair.name, pair.content, collector);
				}
				c++;
			}
						
			//collector.sortStringKey();
			
			StringBuffer pContents[] = Partitioner.partition(collector.collection,numPartitions);
			
			//System.out.println(pContents.length);
			
			/* partition */
			String partitions[] = new String[numPartitions];
			String suffix = "-" + chunks.get(0);
			for(i = 0; i < numPartitions; i++) {
				partitions[i] = jid+"partition"+i+suffix;
				
				//System.out.println(pContents[i]);
				Util.writeBinaryToFile(pContents[i].toString().getBytes("UTF-8"), partitionPath+partitions[i]);
				
				//System.out.println(partitions[i]);
			}
			
//			System.out.println("phase 4:succeed!");
			
		} catch (RemoteException e) {
			e.printStackTrace();
			System.exit(2);
		} catch (NotBoundException e) {
			e.printStackTrace();
			System.exit(2);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		} 

		
		
	}
	
	
}
