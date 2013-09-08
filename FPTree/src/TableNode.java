
public class TableNode {
	private Integer nameItem;
	private TableNode next;
	private int count = -1;// total number of treenode with the same name
	private int height = -1;// based on frequency
	private int loc = -1;//location in FList, lower loc represents high fre
	private TreeNode firstTreeNode;
	private TreeNode lastTreeNode;
	
	public TableNode(int nameItem, int height, int location, int count){
		this.nameItem = nameItem; 
		this.count = count;
		this.height = height;
		this.loc = location;
		next = null;
		firstTreeNode = null;
		lastTreeNode = null;
	}
	
	public TableNode(){
		this.nameItem = null; 
		next = null;
		firstTreeNode = null;
		lastTreeNode = null;
	}
	
	//frequency
	public void setLocation(int loc){
		this.loc = loc;
	}
	public int getLocation(){
		return this.loc;
	}
	// count
	public void setCount(int count){
		this.count = count;
	}
	
	public void addCount(){
		this.count++;
	}
	public int getCount(){
		return this.count;
	}
	//height
	public int getHeight(){
		return this.height;
	}
	public void setHeight(int height){
		this.height = height;
	}
	
	//firstTreeNode
	public void setFirst(TreeNode first){
		this.firstTreeNode = first;
	}
	public boolean hasFirst(){
		return this.firstTreeNode != null;
	}
	public TreeNode getFirst(){
		return this.firstTreeNode;
	}
	//lastTreeNode
	public void setLast(TreeNode last){
		this.lastTreeNode = last;
		}
	public boolean hasLast(){
		return this.lastTreeNode != null;
	}
	public TreeNode getLast(){
		return this.lastTreeNode;
	}
	//name
	public void setName(Integer name){
		nameItem = name;
	}
	public Integer getName(){
		return nameItem;
	}
	// next TableNode
	public void setNext(TableNode next){
		this.next = next;
	}
	public TableNode getNext(){
		return this.next;
	}
	public boolean hasNext(){
		return this.next != null;
	}
	
}
