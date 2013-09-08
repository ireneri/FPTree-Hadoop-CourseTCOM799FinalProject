


import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
	/*
	 * Author: Liangchen Li
	 * Email: liliangc@seas.upenn.edu
	 */
	public class ItemMining extends Configured implements Tool {
	    /**
	     * Mapper part: split the Time string and filter out the hours
	     * Then, hours are used as the keys for reducer
	     */
		public static class ItemMiningMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, IntWritable, Text> {
			// load Flist and Glist( not finished yet!!)
			private Path[] localFiles;
			private ArrayList<Integer> FList = new ArrayList<Integer>();
			private Map<Integer, Integer> GList = new HashMap<Integer, Integer>();
			private int Q;
			// load GList and FList
			@Override
			public void configure(JobConf job) {
				// Get the cached archives/files
				try {
					
					System.out.println("In the configuration of Item Minging");
					Q = Integer.parseInt(job.get("NumberOfGroups"));
					
					localFiles = DistributedCache.getLocalCacheFiles(job);
					FileSystem fs = FileSystem.getLocal(job);
					FSDataInputStream inF = fs.open(localFiles[0]);
					/*BufferedReader inFList = new BufferedReader(new InputStreamReader(inF));
					String strItem = null;
					while ((strItem = inFList.readLine()) != null){
						if(strItem.startsWith("\uFEFF")){
							
							System.out.println("ROM");
						}
					
						System.out.println("strItem " + strItem);
						FList.add(Integer.parseInt(strItem.trim()));
					}*/					
					
					byte[] byteItem = null;
					String strItem = null;
					
					// get the length of FList
					byteItem =  inF.readUTF().getBytes("UTF-8");
					strItem = new String(byteItem, "UTF-8");
					
					int length = Integer.parseInt(strItem.trim());
					
					for (int i = 0; i < length; i++){
						//byteItem = inF.readUTF().getBytes("UTF-8");
						//strItem = new String(byteItem, "UTF-8");	
						strItem = inF.readUTF();
						FList.add(Integer.parseInt(strItem));
					}
					
					inF.close();
					
					FSDataInputStream inG = fs.open(localFiles[1]);
					//BufferedReader inGList = new BufferedReader(new InputStreamReader(inG));
					
					byte[] byteItemGList = null;
					String strItemGList = null;
					// get the length of GList
					byteItemGList =  inG.readUTF().getBytes("UTF-8");					
					strItemGList = new String(byteItemGList, "UTF-8");
					int lengthGList = Integer.parseInt(strItemGList.trim());
					for (int i = 0; i < lengthGList; i++){
						byteItemGList = inG.readUTF().getBytes("UTF-8");
						strItemGList = new String(byteItemGList, "UTF-8");
						String[] KeyValue = strItemGList.split(" ");
						Integer key = Integer.parseInt(KeyValue[0].trim());
						Integer value = Integer.parseInt(KeyValue[1].trim());
						System.out.println("key: " + key);
						System.out.println("value: " + value);
						GList.put(key, value);
					}
					
					 inG.close();
					 
				} catch (IOException e) {
					
					e.printStackTrace();
				}	
			}		    
			
			public void map(LongWritable key, Text value,
					OutputCollector<IntWritable, Text> output, Reporter report)
					throws IOException {
				System.out.println("In the map of ItemMining");
				// sort transaction according to FList
				String[] strItem = value.toString().split(" ");
				// record item and its location in FList
				Map<Integer, Integer> mapLocation = new HashMap<Integer, Integer>();
				//record the Item that appears in FList 
				Integer[] arrayItem = new Integer[strItem.length];
				
				int index = 0;
				for (int i = 0; i < strItem.length; i++){
					Integer intItem = Integer.parseInt(strItem[i]);
					if (FList.contains(intItem)){
						arrayItem[index] = intItem;
						mapLocation.put(intItem, FList.indexOf(intItem));
						index++;
					}
				}
			
				// change GID according to hashing code for each item
				// sort freItem, fre from high to low
				
				int listLength = mapLocation.size();
				// similar to arrayItem, with an appropriate length
				Integer[] freTempItem = new Integer[listLength];
				// item and its gid
				Integer[] gidItem = new Integer[listLength];								
				Integer[] freItem = new Integer[listLength];
				
				for (int i = 0; i < listLength; i++){
					freTempItem[i] = arrayItem[i];
				}
				// after this locItem sorted from high to low according to their loc
				MapSort.quicksort(mapLocation, freTempItem, 0, mapLocation.size() - 1);
			
				boolean[] signGroup = new boolean[Q + 1];// same number with number of Group, 
				//true means this transaction has item that belonged to this group and has not been processed, 
				//it 's default to false, its location Q is always false
				for (int i = 0; i < listLength; i++){
					gidItem[i] = Q;// in case of some item is null, thus has null in gidItem
				}
				
				for ( int i = 0; i < listLength; i++){
					gidItem[mapLocation.size() - 1 - i] = GList.get(freTempItem[i]);
					freItem[mapLocation.size() - 1 - i] = freTempItem[i];
					signGroup[GList.get(freTempItem[i])] = true;
				}
				/*System.out.println("Maplocation:" + mapLocation);
				for (int i = 0; i < freItem.length; i++){
					System.out.println(freItem[i] + " ");
				}*/
							
				signGroup[Q] = false;// if there is an item that is not in this transaction, its gid is set to Q
				Text outputValues = new Text();
				// for each gid, find and output
				for (int i = listLength - 1; i >= 0; i--){
					// process each group once, output a1, a2, ..., ai
					if (signGroup[gidItem[i]]){
						ArrayList<Integer> listItem = new ArrayList<Integer>();// output list
						for ( int j = 0; j <= i; j++){
							listItem.add(freItem[j]);
						}
					
						//System.out.println("Output list: " +listItem);
						outputValues.set(listItem.toString());
						IntWritable gidWritable = new IntWritable(gidItem[i]);
						output.collect(gidWritable, outputValues);
						signGroup[gidItem[i]] = false;
						outputValues.clear();
						listItem.clear();
					}
				}
			}		
		}
	    /*
	     * Reducer part: sum up the flow for each hours
	     */
		public static class ItemMiningReducer extends MapReduceBase
		implements Reducer<IntWritable, Text, Text, IntWritable> {
		
			private Path[] localFiles;
			private Map<Integer, Integer> GList = new HashMap<Integer, Integer>();
			private ArrayList<Integer> FList = new ArrayList<Integer>();
			private int K;// number of fre pattern we save
			
			private TreeNode curTreeNode;
			private TableNode curTableNode;
			private TreeNode parTreeNode;
			
			// load GList and FList
			public void configure(JobConf job) {
				// Get the cached archives/files
				try {	
					K = Integer.parseInt(job.get("K"));
					localFiles = DistributedCache.getLocalCacheFiles(job);
					FileSystem fs = FileSystem.getLocal(job);
					FSDataInputStream inF = fs.open(localFiles[0]);

					byte[] byteItem = null;
					String strItem = null;
					
					// get the length of FList
					byteItem =  inF.readUTF().getBytes("UTF-8");
					strItem = new String(byteItem, "UTF-8");
					int length = Integer.parseInt(strItem.trim());
					
					for (int i = 0; i < length; i++){
						byteItem = inF.readUTF().getBytes("UTF-8");
						strItem = new String(byteItem, "UTF-8");						
						FList.add(Integer.parseInt(strItem.trim()));
					}
					
					inF.close();
					
					FSDataInputStream inG = fs.open(localFiles[1]);
					//BufferedReader inGList = new BufferedReader(new InputStreamReader(inG));
					
					byte[] byteItemGList = null;
					String strItemGList = null;
					// get the length of GList
					byteItemGList =  inG.readUTF().getBytes("UTF-8");					
					strItemGList = new String(byteItemGList, "UTF-8");
					int lengthGList = Integer.parseInt(strItemGList.trim());
					for (int i = 0; i < lengthGList; i++){
						byteItemGList = inG.readUTF().getBytes("UTF-8");
						strItemGList = new String(byteItemGList, "UTF-8");
						String[] KeyValue = strItemGList.split(" ");
						Integer key = Integer.parseInt(KeyValue[0].trim());
						Integer value = Integer.parseInt(KeyValue[1].trim());
						System.out.println("key: " + key);
						System.out.println("value: " + value);
						GList.put(key, value);
					}
					
					 inG.close();
					 
				} catch (IOException e) {
					
					e.printStackTrace();
				}	
			}
			
			public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<Text, IntWritable> output, Reporter reporter)
					throws IOException {
				ArrayList<Integer> nowGList = new ArrayList<Integer>();
				// generate nowGList
				
				for (Entry<Integer, Integer> entry : GList.entrySet()){
					if (entry.getValue() ==  key.get()){
						nowGList.add(entry.getKey());
					}
				}
				//System.out.println("NowGList " + nowGList.toString());
				//System.out.println("GList: " + GList.toString());
				LocalFPTree fpTree = new LocalFPTree();
				// for each transaction
				while (values.hasNext()){
					Text next = values.next();
					String strNext = next.toString();
					String[] transaction = strNext.substring(1, strNext.length() - 1).split(", ");
			
					ArrayList<Integer> listTransaction = new ArrayList<Integer>();
					for (int i = 0; i < transaction.length; i++){
						listTransaction.add(Integer.parseInt(transaction[i]));
					}
					
					//System.out.println("transaction list: " + listTransaction.toString());
					// input frequency for each transaction is 1
					FPTreeBuild(fpTree, listTransaction, 1);
					
				}
				TableNode test = fpTree.rootTableNode;
				while (test.hasNext()){
					test = test.getNext();// iterate table node
					TreeNode testTree = test.getFirst();
					while (true){
						if (testTree == null){
							break;
						}
						/*System.out.println("Name: " + testTree.getName()
								+ ";Height: " + testTree.getHeight()
								+ ";Count: " + testTree.getCount());
						*/
						testTree = testTree.getNextSameItem();
					}
				}
								
				PriorityQueue HP;
				Iterator<Integer> it = nowGList.iterator();
				ArrayList<Integer> pattern; 
				
				while (it.hasNext()){
					int nowItem = it.next();
					HP = new PriorityQueue(nowItem, K);
					pattern = new ArrayList<Integer>();
					pattern.add(nowItem);
					
					/*System.out.println("First: item name " + nowItem 
							+ ", HP name: " + HP.getName());
					*/
					TopKFPGrowth(fpTree, pattern, nowItem, HP);
					//System.out.println("HP and patterns: item " + HP.getName());
					//System.out.println("HP length: " + HP.getLength());
					
					int HPLength = HP.getLength();

					for (int i = 0; i < HPLength; i++){
						FrequentPattern fp = HP.getMinPattern();	
						IntWritable support = new IntWritable(fp.getSupport());
						Text textPattern = new Text(fp.getPattern().toString()); 
						output.collect(textPattern, support);
					}					
				}
				
			}// end of reducer

			private void TopKFPGrowth(LocalFPTree fpTree, ArrayList<Integer> pattern, int nowItem,
					PriorityQueue HP) {
				// look up nowItem in the table
				 
				TableNode curTa = fpTree.rootTableNode;
				while (curTa.hasNext()) {
					curTa = curTa.getNext();
					if (curTa.getName().intValue() == nowItem){
						//System.out.println("Get Item: " + curTa.getName().intValue());
						break;
					}
				}							
				// iterate from first node
				TreeNode curTree = curTa.getFirst();
				// key is item, value is support
				Collection<ArrayList<Integer>> pathSet = new HashSet<ArrayList<Integer>>();
				HashSet<Integer> itemSet = new HashSet<Integer>();
				TreeNode upTree;
				int numberOfPath = 0;
				while (curTree != null){
					// horizon, for the someItemNode
					numberOfPath++;
					upTree = curTree;
					int support = curTree.getCount();
					upTree = upTree.getParent();// don't count for nowitem
					ArrayList<Integer> listPath = new ArrayList<Integer>();
					listPath.add(support);// the first number of the listPath is its support
					while(upTree.hasParent()){// we don't add root into the path
						// updown, find a path with getParent
						
						listPath.add(upTree.getName());
						itemSet.add(upTree.getName());// add all the item in
						upTree = upTree.getParent();
						
					}
					pathSet.add(listPath);
					curTree = curTree.getNextSameItem();
				}
				
				// find all kinds of patterns and its support, add to the priority queue
				//System.out.println("Number of Path: " + numberOfPath);
				
				if (numberOfPath == 1){
					// if single path
					ArrayList<Integer> single = pathSet.iterator().next();	
					
					//System.out.println("Single path: support + path " + single.toString());
					if (single.size() == 1){
						//System.out.println("In single: No extra pattern, return!");
						return;
					}
					//generate frequent pattern
					Integer[] items = new Integer[single.size() - 1];
					int i = 0;
					// get the array of items
					Iterator<Integer> itSingle = single.iterator();
					int support = itSingle.next();
					while (itSingle.hasNext()){
						items[i] = itSingle.next();
						i++;
					}
					int startPoint = 0;
					
					
					generatePattern(nowItem, startPoint, HP, support, items, pattern);
					
				} else {
					//System.out.println("Now in the multiple path!");
					// if multiple path 					
					//build conditional pattern tree for nowItem
					LocalFPTree fpTreeNowItem = new LocalFPTree();
					Iterator<ArrayList<Integer>> itTransaction = pathSet.iterator();
					
					while (itTransaction.hasNext()) {
						ArrayList<Integer> listTransaction = itTransaction.next();// from path set
						
						//System.out.println("Transaction: support + path" + listTransaction.toString());
						if (listTransaction.size() == 1){
							//System.out.println("In multiple: No extra pattern, continue!");
							continue;
						}
						int support = listTransaction.get(0);// first one is the support
						// now the list is inverted because it's from child to parent
						Integer[] invertList = new Integer[listTransaction.size() - 1];
						for (int i = 1; i < listTransaction.size(); i++){
							invertList[i - 1] = listTransaction.get(i);
						}
						ArrayList<Integer> transaction = new ArrayList<Integer>();
						for (int i = 0; i < invertList.length; i++){
							transaction.add(invertList[invertList.length - 1 -i]);
						}
						// build conditional pattern tree
						FPTreeBuild(fpTreeNowItem, transaction, support);
					}
					// test generate table
					
					TableNode test = fpTreeNowItem.rootTableNode;
					//System.out.println("Conditional pattern tree!\n");
					
					while (test.hasNext()){
						test = test.getNext();// iterate table node
						TreeNode testTree = test.getFirst();
						while (true){
							if (testTree == null){
								break;
							}
							/*System.out.println("Name: " + testTree.getName()
									+ ";Height: " + testTree.getHeight()
									+ ";Count: " + testTree.getCount());
							*/
							testTree = testTree.getNextSameItem();
						}
					}
					// for each table node, generate frequent pattern
					TableNode curNode = fpTreeNowItem.rootTableNode;
					while (curNode.hasNext()){
						curNode = curNode.getNext();
						// add item to pattern
						int newItem = curNode.getName();
						//System.out.println("Multiple Path: New Item added to the pattern: " + newItem);
						ArrayList<Integer> newPattern = new ArrayList<Integer>(pattern);
						newPattern.add(newItem);
						/*System.out.println("Multiple Path: now pattern " + newPattern.toString()
								+ "Support: " + curNode.getCount());
						*/
						FrequentPattern fp = new FrequentPattern(newPattern, curNode.getCount());
						HP.add(fp);	// add pattern into priority queue
						// call tree growth
						TopKFPGrowth(fpTreeNowItem, newPattern, newItem, HP);
					}
				}
			}
			private void generatePattern(int nowItem, int startPoint, PriorityQueue HP,
					int support, Integer[] items, ArrayList<Integer> pattern) {
				// find all the combination of items
				/*System.out.println("Singel path pattern generation: NowItem " + nowItem
						+ " startPoint " + startPoint + " support " + support
						+ " Pattern " + pattern.toString());
				for (int i = 0; i < items.length; i++){
					System.out.println("Item " + items[i] + " ");
				}
				*/
				for (int i = startPoint; i < items.length; i++){
					ArrayList<Integer> onePattern = new ArrayList<Integer>(pattern);
					
					onePattern.add(items[i]);
					
					//System.out.println("Single Path: new Pattern: " + onePattern.toString());
					//System.out.println("Single Path: new item " + items[i]);
					
					FrequentPattern fp = new FrequentPattern(onePattern, support);
					HP.add(fp);
					generatePattern(nowItem, i + 1, HP, support, items, onePattern);
				}
			}

			//build the fptree using one transaction
			private void FPTreeBuild(LocalFPTree fpTree, ArrayList<Integer> next, int frequency) {
				parTreeNode = fpTree.rootTreeNode;
				curTreeNode = fpTree.rootTreeNode.getChild();
				curTableNode = fpTree.rootTableNode;
				
				Iterator<Integer> it = next.iterator();
				
				while ( it.hasNext()){
					Integer itNext = it.next();	
					
					//System.out.println("new Item " + itNext);
					//System.out.println("Current Table Node: " + curTableNode.getName());
					
					insertTree(itNext, frequency);
				}
		
			}
			// insert every item into the tree
			// current node:current node
			// child node: the first node of the parent layer
			// brother nodes: brothers of the current nodes
			private void insertTree(Integer item, int frequency) {
				TreeNode broTreeNode;
				TreeNode lastBroTreeNode = null;
				boolean treeNodeExist = false;
				boolean tableNodeExist = false;
				
				if (curTreeNode != null){
					// check 
					/*System.out.println("Current Tree node: " + curTreeNode.getName() 
							+" in height: " + curTreeNode.getHeight() 
							+ " . New Item: " + item);
					*/
					if (curTreeNode.equals(item)){
						curTreeNode.setCount(curTreeNode.getCount() + frequency);
						
						//System.out.println("count in current" + curTreeNode.getCount());
						
						parTreeNode = curTreeNode;
						curTreeNode = curTreeNode.getChild();

						treeNodeExist = true;// exist in child node

					} else{// current node exists but not match
						broTreeNode = curTreeNode.getBrother();
						lastBroTreeNode = curTreeNode;
						
						while (broTreeNode != null){
							//look up brothers
							if (broTreeNode.equals(item)){
								broTreeNode.setCount(broTreeNode.getCount() + frequency);
								
								//System.out.println("In brother: count " + broTreeNode.getCount());
								
								treeNodeExist = true;// exist in brother node							
								curTreeNode = broTreeNode.getChild();
								parTreeNode = broTreeNode;
								
								break;
							}
							
							lastBroTreeNode = broTreeNode;
							broTreeNode = broTreeNode.getBrother();
						}
						
						//System.out.println("treeNodeExist: " + treeNodeExist);
						
						if (!treeNodeExist){
							// doesn't exist in brothers, add a node at the end the brothers
							// current brother node is null
							//System.out.println("Create brother of " + lastBroTreeNode.getName() + ": " + item);
							broTreeNode = new TreeNode(item, curTreeNode.getHeight(), frequency);
							
							lastBroTreeNode.setBrother(broTreeNode);//for "look down"
							broTreeNode.setParent(parTreeNode);// for "look up"
							
							curTreeNode = broTreeNode.getChild();
							parTreeNode = broTreeNode;
						}
						
					}
				} else{// current node doesn't exist, create 
					
					//System.out.println("Create child node of " + parTreeNode.getName() + ": "+ item);
					
					curTreeNode = new TreeNode(item, parTreeNode.getHeight() + 1, frequency);
					
					curTreeNode.setParent(parTreeNode);// for up
					parTreeNode.setChild(curTreeNode);// for down
					
					parTreeNode = curTreeNode;
					curTreeNode = curTreeNode.getChild();
				}
				
				//System.out.println(treeNodeExist);
				
				if (treeNodeExist){
					// treeNodeExist means tableNode exists, we only update its count(no new treeNode added
					while (curTableNode.hasNext()){
						curTableNode = curTableNode.getNext();
						if (curTableNode.getName() == item.intValue()){
							curTableNode.setCount(curTableNode.getCount() + frequency);
							//System.out.println("TableCount of " + curTableNode.getName() + ": " + curTableNode.getCount());
							break;
						}
					}
				} else{// not sure tablenNode exists or not, but need to add a last to the end
					int locOfItem = FList.indexOf(item);
					
					//System.out.println("before checking tablenode: " + curTableNode.getName());
					
					while (true){
						
						/*System.out.println("TableNode: " + curTableNode.getName() 
								+ " location:" + curTableNode.getLocation()
								+ " new: " + item + " loc:" + locOfItem);
						*/
						// check current value: if match, addcount and break
						if (curTableNode.getName() == item.intValue()){
							curTableNode.setCount(curTableNode.getCount() + frequency);
							TreeNode last = curTableNode.getLast();
							
							/*System.out.println("Tablecount " + curTableNode.getCount() 
									+ "lastname: " + last.getName());
							*/
							last.setNextSameItem(parTreeNode);// last node linked to tree node, already update
							curTableNode.setLast(parTreeNode);// last node added as tablenode.last, already update
							//System.out.println("Table exists, add last");
							tableNodeExist = true;
							break;
						}
						// check next, if not, break, add to the end
						if (!curTableNode.hasNext()){
							//System.out.println("Add to the end");
							break;
						}
						/*System.out.println("Next TableNode: " + curTableNode.getNext().getName() 
								+ " location:" + curTableNode.getNext().getLocation()
								+ " new: " + item + " loc:" + locOfItem);*/
						
						if (curTableNode.getNext().getLocation() > locOfItem){
							//System.out.println("need to insert table at the end of " + curTableNode.getName());
							break;
						}
						
						curTableNode = curTableNode.getNext();
					}
					
					if (!tableNodeExist){
						//System.out.println("Create Table: " + item);
						TableNode newNode = new TableNode(item, curTableNode.getHeight() + 1, locOfItem, frequency);
						newNode.setNext(curTableNode.getNext());
						newNode.setFirst(parTreeNode);// when operating here, current node has been the child
						newNode.setLast(parTreeNode);
						curTableNode.setNext(newNode);
						//System.out.println("Create table " + newNode.getName() + "after " + curTableNode.getName() );
					}
				}
			}
		}
	    /**
	     * Jab configurations
	     */
		public int run(String[] args) throws Exception {
			
			JobConf conf = new JobConf(new Configuration(), ItemMining.class);
			DistributedCache.addCacheFile(new URI("/user/leon/output1/FList.txt#FList.txt"), conf);
			DistributedCache.addCacheFile(new URI("/user/leon/output1/GList.txt#GList.txt"), conf);
			//DistributedCache.addCacheFile(new Path("/user/leon/output1/FList.txt").toUri(), conf);
			//DistributedCache.addCacheFile(new Path("/user/leon/output1/GList.txt").toUri(), conf);
			conf.set("NumberOfGroups", args[3]);
			conf.set("K", args[4]);
			conf.setJobName("ItemMining");
			
			conf.setBoolean("mapred.output.compress", false); 
			
			conf.setMapperClass(ItemMiningMapper.class);
			conf.setReducerClass(ItemMiningReducer.class);
			
			conf.setInputFormat(TextInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);
	    	
			conf.setOutputKeyClass(IntWritable.class);
			conf.setOutputValueClass(Text.class);
			
			FileInputFormat.setInputPaths(conf, new Path(args[0]));
			FileOutputFormat.setOutputPath(conf, new Path("output2"));
	
			JobClient.runJob(conf);
	        return 0;

		}

		public static void main(String[] args) throws Exception {
			// ItemCount(first MapReduce)
			// int res = ToolRunner.run(new Configuration(), new ItemMining(), args);
			}
	}



