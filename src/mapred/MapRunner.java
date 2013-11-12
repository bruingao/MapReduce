package mapred;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashSet;
import java.util.List;

import Common.Util;
import Common.Collector;
import Common.Partitioner;

import dfs.DataNodeI;
import format.inputFormatAbs;
import format.inputFormatAbs.kvPair;

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
		
		String mapperName = args[0];
		
		int jid = Integer.parseInt(args[1]);
		
		int numPartitions = Integer.parseInt(args[2]);
		
		String partitionPath = args[3];
				
		int numOfChunks = Integer.parseInt(args[4]);
		
		String filename = args[5];
		
		String inputformat = args[6];
		
		HashSet<Integer> chunks = new HashSet<Integer>();
		
		int i = 0;
		while (i < numOfChunks) {
			chunks.add(Integer.parseInt(args[7 + i]));
			i++;
		}
		
		try {
			Class<Mapper> ma = (Class<Mapper>) Class.forName(mapperName);
		
			Constructor<Mapper> cma = ma.getConstructor();
			
			mapper = cma.newInstance();
			
			String content[] = new String[numOfChunks];
			
			/* read file */
			int c = 0;
			
			Collector collector = new Collector();
			
			for (int ck : chunks) {
				String datanodeHost = args[7 + numOfChunks];
				
				int regPort = Integer.parseInt(args[8 + numOfChunks]);
				
				String service = args[9 + numOfChunks];
				
				
					Registry reg = LocateRegistry.getRegistry(datanodeHost, regPort);
					DataNodeI datanode = (DataNodeI)reg.lookup(service);
					content[c] = new String(datanode.read(filename+ck),"UTF-8");
					
					/* produce key pair */
					Class<inputFormatAbs> iFormat = (Class<inputFormatAbs>) Class.forName(inputformat);
					Constructor<inputFormatAbs> constuctor = iFormat.getConstructor(String.class);
					
					inputFormatAbs iformat = constuctor.newInstance(content[c]);
					
					List<kvPair> pairs = iformat.getkvPairs();
					
					for(kvPair pair : pairs) {
						mapper.map(pair.key, pair.value, collector);
					}
			}
			
			collector.sortStringKey();
			
			String pContents[] = Partitioner.partition(collector.collection, collector.uniqueKeys, numPartitions);
			
			/* partition */
			String partitions[] = new String[numPartitions];
			String suffix = "-" + chunks.toArray()[0].toString();
			for(i = 0; i < numPartitions; i++) {
				partitions[i] = jid+"partition"+i+suffix;
				
				Util.writeBinaryToFile(pContents[i].getBytes("UTF-8"), partitionPath+partitions[i]);
				
				System.out.println(partitions[i]);
			}
			
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
		
	}
}
