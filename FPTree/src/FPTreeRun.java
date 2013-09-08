import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;


public class FPTreeRun {
	private ArrayList<Integer> FList;
	private Map<Integer, Integer> GList;
	private Map<Integer, Integer> mapList;
	
	public FPTreeRun(String args[]){
		mapList = new TreeMap<Integer, Integer>();
		GList = new HashMap<Integer, Integer>();
		FList = new ArrayList<Integer>();
	}

	private void runJob(String[] args) throws Exception {
		
		// ItemCount(first MapReduce)
		ToolRunner.run(new Configuration(), new ItemCount(), args);
		
		// list input		 
		fileReadIntoMap("output1", args[2], mapList);
		
		//create Flist
		createFList();
		
		// use to output FList or error info				
		FileSystem hdfs = FileSystem.get(new Configuration());
		Path path = new Path("output1/FList.txt");
		FSDataOutputStream outputFList = hdfs.create(path);					
		
		// can't use FileWriter flistOutput = new FileWriter("output/FList.txt");			
		
		if (FList.size() == 0){
			outputFList.writeUTF("No item exists in Flist");
			return;
		}
		//output Flist as a integer array
		Integer[] key = FList.toArray(new Integer[0]);
		MapSort.quicksort(mapList, key, 0, FList.size() - 1);
		
		for (int i = 0; i < key.length;i++){
			FList.set(i, key[i]);
		}
		
		// divide F-list to G-list, Q is the number of Groups
		int Q = Integer.parseInt(args[3]);
		partition(FList, Q);
				
		mapWriteIntoFile(GList, "output1/GList.txt");
		// for readUTF, there will be EOF exception
		int length = FList.size();
		outputFList.writeUTF(length + "\n");
		//writing Flist to the file			
		for (Integer intFList:FList){
			// similar to writeString
			outputFList.writeUTF(intFList.toString());
		}
		outputFList.close();
		// parallel FP-Growth(second MapReduce)
		ToolRunner.run(new Configuration(), new ItemMining(), args);
		
		int res = ToolRunner.run(new Configuration(), new ItemPatternFilter(), args);
		
		System.exit(res);
	}
	private void createFList() {
		// item sort and filter to get F-List
		Object[] objKey = mapList.keySet().toArray();				
		for (int i = 0; i < objKey.length; i++){
			FList.add((Integer) objKey[i]);
		}	
	}

	/*
	 * first build group list than contains HashMap in each entry, which is used to store items and their frequency
	 */
	private void partition(ArrayList<Integer> FList, int Q) throws IOException {
		gGroup[] groupList = new gGroup[Q];
		PriorityHeapQueue gBanlanceTree = new PriorityHeapQueue(Q);
		for (int i = 0; i < Q; i++){
			groupList[i] = new gGroup(i);
			//System.out.println(FList.toString());
			groupList[i].putItem(FList.get(i), mapList);//put itemID and item load
			gBanlanceTree.add(groupList[i]);
			GList.put(FList.get(i), i);// item/GID
			
		}
		
		for (int i = Q; i < FList.size(); i++){
			int intMinID = gBanlanceTree.getMinLoadID();
			GList.put(FList.get(i), intMinID);// add item/GID pair
			groupList[intMinID].putItem(FList.get(i), mapList);			
			gBanlanceTree.addLoadToMin(groupList[intMinID]);// we know where the min load is					
		}	
	}

	private void fileReadIntoMap(String strFileName, String strThreshold, Map<Integer, Integer> list) throws IOException {
		//strFileName = "/user/leon/" + strFileName;
		FileSystem hdfs = FileSystem.get(new Configuration());
		Path path = new Path(strFileName);
		FileStatus[] status = hdfs.listStatus(path);
		int intThreshold = Integer.parseInt(strThreshold);
		
		for (int i = 0; i < status.length; i++){
			if (status[i].isDir()){
				continue;
			}
			
			FSDataInputStream fsInput = hdfs.open(status[i].getPath());
			BufferedReader buffInput = new BufferedReader(new InputStreamReader(fsInput));
			String line = null;			
			while((line = buffInput.readLine()) != null){
				String[] strInput = line.split("\t", -1);
				// get the Item and its frequency
				System.out.println(line);
				int intItem = Integer.parseInt(strInput[0]);
				int intFreq = Integer.parseInt(strInput[1]);
				// if frequency is bigger than threshold, put it into map
				if (intFreq >= intThreshold ){
					list.put(intItem, intFreq);
				}
			}		
		}
		
	}
	
	private void mapWriteIntoFile(Map<Integer, Integer> map, String strOutputName) throws IOException{
		//FileWriter fileOutput = new FileWriter(strOutputName);
		FileSystem hdfs = FileSystem.get(new Configuration());
		Path path = new Path(strOutputName);
		FSDataOutputStream outputMap = hdfs.create(path);
		int length = map.size();
		outputMap.writeUTF(length + "\n");
		for (Map.Entry<Integer, Integer> entry:map.entrySet()){
			outputMap.writeUTF(entry.getKey().toString() + " " + entry.getValue().toString() + "\n");
		}
		outputMap.close();
	}
	
	/*
	 * args input, output, threshold, number of group Q, max of priority queue,
	 * 
	 */
	public static void main(String[] args) throws Exception {
		FPTreeRun objFPTree = new FPTreeRun(args);
		objFPTree.runJob(args);		
	}	
}
