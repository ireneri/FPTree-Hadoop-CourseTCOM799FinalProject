import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

// keep the min in the top
// try to implement priority queue
public class PriorityHeapQueue {
	public int length;
	int capacity;
	private int[] arrayID;// GID
	public Map<Integer, Integer> loadList;// GID and its load
	
	public PriorityHeapQueue(int Q){
		length = 0;
		capacity = Q;
		arrayID = new int[capacity + 2]; // if capacity is Q, we need Q + 2 slots, 0 is empty
		loadList = new HashMap<Integer, Integer>();
	}
	
	public void add(gGroup gItem){
		
		arrayID[length + 1] = gItem.getGID();// add GID to the end
		loadList.put(gItem.getGID(), gItem.getTotalLoad());// add GID and its first load
		
		swim(length + 1);
		if (isFull()){
			delMin();
		}
		length++;
		
	}
	
	public int getMinLoadID(){
		return arrayID[1];
	}
	
	public void addLoadToMin(gGroup gItem){

		loadList.put(arrayID[1], gItem.getTotalLoad());// intMinID is arrayID[1]
		sink(1);

	}
	
	private void sink(int k) {
		int j = 2 * k;
		while ( j <= length + 1){
			if (j < length + 1 && less(j + 1, j)){
				j++;
			} 
			if (less(j, k)){
				exch(j, k);
			} else {
				break;
			}
			
			k = j;
		}
		
	}

	private void delMin() {
		// TODO Auto-generated method stub
		
	}

	private void swim(int k) {
		
		while ( k > 1 && less(k, k/2)){
			exch(k/2, k);
			k = k/2;
		}
		
	}

	public boolean isFull(){
		return length == capacity - 1;
	}
	
	public boolean isEmpty(){
		return length == 0;
	}
	
	private boolean less(int i, int j){
		if (loadList.get(arrayID[i]) < loadList.get(arrayID[j])){
			return true;
		}
		return false;
	}
	
	private void exch(int i, int j){
		int temp = arrayID[i];
		arrayID[i] = arrayID[j];
		arrayID[j] = temp;
	}
	
}
