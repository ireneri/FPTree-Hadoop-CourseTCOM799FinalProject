import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


public class gGroup {

	private ArrayList<Integer> arrayItemID;
	private int intGID;// GID
	private int load;
	public gGroup(int ID){
		intGID = ID;
		arrayItemID = new ArrayList<Integer>();
	}
	
	public void putItem(Integer item, Map<Integer, Integer> mapList){
		arrayItemID.add(item);
		load = load + mapList.get(item);
		
	}
	public void setGID(int ID){
		intGID = ID;
	}
	
	public int getGID(){
		return intGID;
	}
	
	public int getTotalLoad(){
		return load;
	}
	
	public String toString(){
		return load + arrayItemID.toString();
	}
	
}

