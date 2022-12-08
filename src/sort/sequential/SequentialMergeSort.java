package sort.sequential;

import java.util.Iterator;
import java.util.LinkedList;

public class SequentialMergeSort {

	public static LinkedList<Integer> mergeSort(LinkedList<Integer> arr) {

		int length = arr.size();
		
		// base case
		if (length < 2) {
			return arr;
		}
		
		// recursive divide-and-conquer case
		else { 
			/* compute the size of the two sub arrays */
			int halfSize = length / 2;

			/* declare these as `left` and `right` arrays */
			LinkedList<Integer> left = new LinkedList<Integer>();
			LinkedList<Integer> right = new LinkedList<Integer>();

			/* populate the left array with values */
			int index = 0;
			Iterator<Integer> it = arr.iterator();
			while (index < halfSize) {
				left.add(it.next());
				index++;
			}

			/* populate the right array with values */
			index = 0;
			while (index < length - halfSize) {
				right.add(it.next());
				index++;
			}

			LinkedList<Integer> resultLeft = mergeSort(left);
			LinkedList<Integer> resultRight = mergeSort(right);

			/* merge the sorted sub arrays */
			return merge(resultLeft, resultRight);
		}
	}

	// to assist mergeSort
	public static LinkedList<Integer> merge(LinkedList<Integer> left, LinkedList<Integer> right) {
		int lindex = 0;
		int rindex = 0;
		LinkedList<Integer> mergedList = new LinkedList<Integer>();

		/*
		 * step 1: compare elementwise `left` and `right` arrays and write the smaller
		 * of the two values into the `arr` array
		 */
		while (lindex < left.size() && rindex < right.size()) {
			if (left.get(lindex) <= right.get(rindex)) {
				mergedList.add(left.get(lindex));
				lindex++;
			} else {
				mergedList.add(right.get(rindex));
				rindex++;
			}
		}

		// step 2: write any remaining values in the `left` array into the `arr` array
		while (lindex < left.size()) {
			mergedList.add(left.get(lindex));
			lindex++;
		}

		// step 3: write any remaining values in the `right` array into the `arr` array
		while (rindex < right.size()) {
			mergedList.add(right.get(rindex));
			rindex++;
		}

		return mergedList;
	}
	
	public static void benchmark(LinkedList<Integer> numbers) {
		// time before
		long startTime = System.currentTimeMillis();

		// run the parallel benchmark
		mergeSort(numbers);
		
		// time after
		long stopTime = System.currentTimeMillis();
		
		// how long it took
		long time = stopTime - startTime;
		System.out.println("sequential mergeSort\t" + time + "ms");
	}
	
	public static void main(String[] args) {
		LinkedList<Integer> numbers = SortingCommon.randomList(50000);
		benchmark(numbers);
	}
}
