import java.util.Map;


public class MapSort{
	
	public static void quicksort(Map<Integer, Integer> mapList, Integer[] key, int low, int high){
		if ( low >= high) return;
		
		int i = partition(mapList, key, low, high);

		quicksort(mapList, key, low, i - 1);
		quicksort(mapList, key, i + 1, high);
	}

	private static int partition(Map<Integer, Integer> mapList, Integer[] key,
			int low, int high) {
		int intKeyLoc = low;
		int i = low;
		int j = high + 1;
		while (true){
			while (i < high){ 

				if (mapList.get(key[++i]) < mapList.get(key[intKeyLoc])){
					break;
				}
			}
		
			while (low < j){
				if (mapList.get(key[--j]) > mapList.get(key[intKeyLoc])) {
					break;
				}
			}
			if (i >= j) break;
			exchItem(key, i, j);
		}
		
		exchItem(key, j, intKeyLoc);
		
		return j;
	}

	private static void exchItem(Integer[] key, int i, int j) {
		Integer temp = key[i];
		key[i] = key[j];
		key[j] = temp;
		
	}
	
	
}
