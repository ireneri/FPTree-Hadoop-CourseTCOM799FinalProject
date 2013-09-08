
public class TreeNode {
	private Integer nameItem;
	private TreeNode child;
	private TreeNode brother;
	private TreeNode parent;
	private TreeNode nextSameItem;
	
	private int count;
	private int height;
	public TreeNode(int nameItem, int height, int count){
		this.nameItem = nameItem; 
		this.count = count;
		this.height = height;
		child = null;
		brother = null;
		nextSameItem = null;
	}
	
	public TreeNode(){
		this.nameItem = null; 
		child = null;
		brother = null;
		nextSameItem = null;
	}
	// count
	public void setCount(int count){
		this.count = count;
	}
	
	public void addCount(){
		this.count = this.count + 1;
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
	//child
	public void setChild(TreeNode child){
		this.child = child;
	}
	
	public boolean hasChild(){
		return this.child != null;
	}
	
	public TreeNode getChild(){
		return this.child;
	}
	//parent
	public void setParent(TreeNode parent){
		this.parent = parent;
	}
		
	public boolean hasParent(){
		return this.parent != null;
	}
		
	public TreeNode getParent(){
		return this.parent;
	}
	//brother
	public void setBrother(TreeNode brother){
		this.brother = brother;
	}
	public boolean hasBrother(){
		return this.brother != null;
	}
	public TreeNode getBrother(){
		return this.brother;
	}
	//name
	public void setName(Integer name){
		nameItem = name;
	}
	public Integer getName(){
		return nameItem;
	}
	// nextSameItem
	public void setNextSameItem(TreeNode next){
		nextSameItem = next;
	}
	public TreeNode getNextSameItem(){
		return nextSameItem;
	}
	public boolean hasNextSameItem(){
		return nextSameItem != null;
	}
	// tools
	public boolean equals(Integer item){
		return nameItem.equals(item);
	}
}
