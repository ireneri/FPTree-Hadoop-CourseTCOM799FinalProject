import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Writable;


public class ArrayListWritable extends ArrayList<Integer> implements Writable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	ArrayList<Integer> list;
	
	public ArrayListWritable(){
		list = new ArrayList<Integer>();
	}	
	
	@Override
	public void readFields(DataInput in) throws IOException {	
	
		this.clear();
		int numFields = in.readInt();
		if (numFields == 0)
			return;
		try {
				
			for (int i = 0; i < numFields; i++) {
			
				System.out.println(in.readInt());
			}
		
			} catch (Exception e) {
				e.printStackTrace();
			}
			
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		out.writeInt(list.size());
		Iterator<Integer> it = list.iterator();
		while (it.hasNext()){
			out.writeInt(it.next());
		}	

	}
}
