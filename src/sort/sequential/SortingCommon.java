package sort.sequential;
import java.util.LinkedList;

public class SortingCommon {

	public static LinkedList<Integer> randomList(int length) {
		LinkedList<Integer> list = new LinkedList<Integer>();
		for (int i = 0; i < length; i++) {
			// between 1 and 1000
			int min = 1;
			int max = 1000;
			int randomNumber = (int) (Math.random() * (max - 1 + min) + min);
			list.add(randomNumber);
		}
		return list;
	}

	// checks if the array is sorted, useful for the unit tests.
	public static boolean isSorted(LinkedList<Integer> arr) {
		for (int i = 0; i < arr.size() - 1; i++)
			if (arr.get(i) > arr.get(i + 1))
				return false;
		return true;
	}
}
