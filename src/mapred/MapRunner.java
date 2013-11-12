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

import dfs.DataNodeI;
import format.Collector;
import format.inputFormatAbs;
import format.inputFormatAbs.kvPair;

public class MapRunner {
	
	private static Mapper mapper;
	
	public static void main(String[] args) {
		/* mapper classname, jobid, num of partitions, numOfChunks, inputfile, chunks, datanodeHost, regPort, datanode service name */
		
		String mapperName = args[0];
		
		int jid = Integer.parseInt(args[1]);
		
		int numPartitions = Integer.parseInt(args[2]);
				
		Integer numOfChunks = Integer.parseInt(args[3]);
		
		String filename = args[4];
		
		String inputformat = args[5];
		
		HashSet<Integer> chunks = new HashSet<Integer>();
		
		int i = 0;
		while (i < numOfChunks) {
			chunks.add(Integer.parseInt(args[6 + i]));
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
				String datanodeHost = args[6 + numOfChunks];
				
				int regPort = Integer.parseInt(args[7 + numOfChunks]);
				
				String service = args[8 + numOfChunks];
				
				
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
			
			/* collector.sortStringKey(); */
			
			/* partition */
			
			
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

	public static Mapper getMapper() {
		return mapper;
	}

	public static void setMapper(Mapper mapper) {
		MapRunner.mapper = mapper;
	}
}
