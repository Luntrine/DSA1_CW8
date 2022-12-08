package sort.parallel;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;

import sort.sequential.SequentialMergeSort;
import sort.sequential.SortingCommon;
import utils.Benchmark;

/*
 * Merge Sort results
 * ~~~~~~~~~~~~~~~~~~
 * 
 * Before parallelisation:
 * - 1 thread: 3715ms
 * - 2 threads: 3624ms
 * - 4 threads: 3620ms
 * - 8 threads: 3658ms
 * 
 * After parallelisation:
 * - 1 thread: 6397ms
 * - 2 threads: 4530ms
 * - 4 threads: 4097ms
 * - 8 threads: 4091ms
 */

public class ParallelMergeSort extends RecursiveTask<LinkedList<Integer>> {
	LinkedList<Integer> arr;

	public ParallelMergeSort(LinkedList<Integer> arr) {
		this.arr = arr;
	}

	/** Performs a merge sort on an arrayList.
	 * 
	 * Complexity: O(N log(N)).
	 */
	@Override
	protected LinkedList<Integer> compute() {
		int length = arr.size();
		// base case
		if (length < 2) {
			return arr;
		}
		// step case
		else {
			/* compute the size of the two sub arrays */
			int halfSize = length / 2;

			/* declare these as `left` and `right` arrays */
			LinkedList<Integer> left = new LinkedList<Integer>();
			LinkedList<Integer> right = new LinkedList<Integer>();

			/* populate the left array with values */
			Iterator<Integer> it = arr.iterator();
			int index = 0;
			while (index < halfSize) {
				left.add(it.next());
				index++;
			}
			
			// The following code sets the left array to merge sort in a separate
			// CPU core before working on the right array to maximize efficiency.
			ParallelMergeSort mergeLeft = new ParallelMergeSort(left);
			mergeLeft.fork();

			/* populate the right array with values */
			index = 0;
			while (index < length - halfSize) {
				right.add(it.next());
				index++;
			}

			/*
			 * Q1A: profile this sequential implementation.
			 * 
			 * Q1B: replace this with a parallel fork/join approach. (Part of which is above).
			 */
			
			
			// The following code sets the right array to merge sort in the current
			// CPU core.
			ParallelMergeSort mergeRight = new ParallelMergeSort(right);
			LinkedList<Integer> resultRight = mergeRight.compute();
			
			// The following code puts the result from mergeLeft (which was computing
			// in another core) back into the core which mergeRight computed in.
			LinkedList<Integer> resultLeft = mergeLeft.join();
			
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
		 * step 1: compare elementwise left and right lists and write the smaller of the
		 * two values into the mergedList list
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

		// step 2: write any remaining values in the left list into the mergeList list
		while (lindex < left.size()) {
			mergedList.add(left.get(lindex));
			lindex++;
		}

		// step 3: write any remaining values in the right list into the mergeList list
		while (rindex < right.size()) {
			mergedList.add(right.get(rindex));
			rindex++;
		}
		return mergedList;
	}

	/**
	 * Parallel merge sort
	 * 
	 * @param numbers     the list of numbers to be sorted
	 * @param parallelism how many CPU cores to use
	 * @return the sorted list
	 */
	public static LinkedList<Integer> parallelMergeSort(LinkedList<Integer> numbers, int parallelism) {
		ForkJoinPool pool = new ForkJoinPool(parallelism);
		ParallelMergeSort mergeSortTask = new ParallelMergeSort(numbers);
		LinkedList<Integer> result = pool.invoke(mergeSortTask);
		return result;
	}

	/**
	 * Benchmarks parallel merge sort
	 */
	public static void main(String[] args) {
		/* generates a random list */
		LinkedList<Integer> numbers = SortingCommon.randomList(50000);

		/* gets the number of cores in this computer's CPU */
		int cpuCores = Runtime.getRuntime().availableProcessors();

		/*
		 * benchmarks the parallel merge sort for Q2 on 1, 2, 4, .. CPU cores
		 */
		for (int threads = 1; threads <= cpuCores; threads *= 2) {
			Benchmark.parallel(new ParallelMergeSort(numbers), threads);
		}
	}

}