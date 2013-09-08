import java.util.ArrayList;


public class FrequentPattern implements Comparable<FrequentPattern>{
	//private int itemName;
	private ArrayList<Integer> pattern;
	private int support;
	
	public FrequentPattern(ArrayList<Integer> pattern, int support){
		//this.itemName = itemName; 
		this.pattern = pattern;
		this.support = support;
	}
	
	/*public boolean sameNode(int itemName){
		return this.itemName == itemName;
	}*/
	
	// for the same item, check whether is the same pattern
	public boolean equals(ArrayList<Integer> pattern){
		return this.pattern.toString() == pattern.toString();
	}
	
	public int getSupport(){
		return this.support;
	}
	
	public ArrayList<Integer> getPattern(){
		return this.pattern;
	}
	
	public int compareTo(FrequentPattern fp){
		if (this.support > fp.getSupport()){
			return 1;
		} else if (this.support == fp.getSupport()){
			return 0;
		} else {
			return -1;
		}
	}
}
