import java.util.ArrayList;


public class PriorityQueue {
	private int itemName;
	private FrequentPattern[] fpQueue;
	private int length;// current number of array
	private int loc;// current location that we set entry in
	private int capacity;
	
	public PriorityQueue(int itemName, int capacity){
		this.itemName = itemName;
		this.capacity = capacity;
		fpQueue = new FrequentPattern[capacity + 2];// the end should be capacity + 1, the extra one is for adding node
		length = 0;
		loc = length + 1;
	}
		
	public int getName(){
		return this.itemName;
	}
	
	public int getLength(){
		return this.length;
	}
	
	public void add(FrequentPattern fp){
		fpQueue[loc] = fp;// add fp to the end
		swim(loc);// swim to the appropriate location
		length++;
		loc++;
		/*System.out.println("Add: pattern " + fp.getPattern().toString()
				+ " support " + fp.getSupport() + "\n");*/
		
		if (isFull()){
			delMin();
				
			/*System.out.println("Delete: pattern " + min.getPattern().toString()
				+ " support " + min.getSupport());*/
		}
	}
	
	public ArrayList<Integer> getMinSupportPattern(){
		return fpQueue[1].getPattern();
	}
	
	private void sink(int k) {
		
		while ( 2 * k <= length ){
			int j = 2 * k;
			if (j < length  && greater(j , j + 1)){
				j++;
			} 
			if (!greater(k, j)){
				break;
			}		
			exch(k, j);
			k = j;
		}		
	}

	private FrequentPattern delMin() {
		//exch min and last
		FrequentPattern min = fpQueue[1];
		exch(1, loc - 1);
		loc--;
		length--;
		sink(1);
		fpQueue[loc] = null;		
		
		return min;
	}

	private void swim(int k) {
		
		while ( k > 1 && greater(k/2, k)){
			exch(k/2, k);
			k = k/2;
		}
		
	}

	public boolean isFull(){
		// this full means that we have used the extra slot and we need to delete one for future use
		return length == capacity + 1;
	}
	
	public boolean isEmpty(){
		return length == 0;
	}
	
	private boolean greater(int i, int j){
		if (fpQueue[i].getSupport() > fpQueue[j].getSupport()){
			return true;
		}
		return false;
	}
	
	private void exch(int i, int j){
		FrequentPattern temp = fpQueue[i];
		fpQueue[i] = fpQueue[j];
		fpQueue[j] = temp;
	}	
	// for mapreduce output
	public FrequentPattern getMinPattern(){
		FrequentPattern fp = delMin();
		return fp;
		
	}
	
	public String iterateQueue(){
		String output = this.itemName + ":\n";
		for (int i = 1; i <= this.length; i++){
			
			output = output + fpQueue[i].getPattern()+ " " 
					+ fpQueue[i].getSupport() + "\n";
		}
		return output;
	}
}
