package sort.parallel;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;

import sort.sequential.SequentialMergeSort;
import sort.sequential.SortingCommon;
import utils.Benchmark;

/*
 * Merge Sort results with thresholding
 * Run on Macbook Pro M1 so parallisation with thresholding is not fantastic on the 4 performance, 4 efficiency cores.
 * ~~~~~~~~~~~~~~~~~~
 *
 * After parallelisation:
 * - 1 thread
 *   - no threshold: 6472ms
 *   - threshold=128: 6277ms
 *   - threshold=512: 6336ms
 *   - threshold=2048: 6299ms
 *   - threshold=8192: 6541ms
 *
 * - 2 threads
 *   - no threshold: 4644ms
 *   - threshold=128: 4510ms
 *   - threshold=512: 4659ms
 *   - threshold=2048: 4585ms
 *   - threshold=8192: 4610ms
 *   
 * - 4 threads
 *   - no threshold: 4498ms
 *   - threshold=128: 4283ms
 *   - threshold=512: 4305ms
 *   - threshold=2048: 4327ms
 *   - threshold=8192: 4202ms
 *   
 *  - 8 threads
 *   - no threshold: 4228ms
 *   - threshold=128: 4259ms
 *   - threshold=512: 4324ms
 *   - threshold=2048: 4250ms
 *   - threshold=8192: 4293ms
 *
 * Parameters of the shortest runtime: (new ParallelMergeSortThreshold(numbers, 8192), 4);
 * - runtime: 4202ms
 * - how many threads: 4
 * - threshold value: 8192
 * Best parallel speedup: 6472ms / 4202ms = 1.54x
 * 
 * Parallelism efficiency: 1.54 / 4 = 0.38 = 38%
*/

public class ParallelMergeSortThreshold extends RecursiveTask<LinkedList<Integer>> {
	LinkedList<Integer> arr;
	int threshold;

	public ParallelMergeSortThreshold(LinkedList<Integer> arr, int threshold) {
		this.arr = arr;
		this.threshold = threshold;
	}

	/** Performs a merge sort on an arrayList.
	 * 
	 * Complexity: O(N log(N)).
	 */
	@Override
	protected LinkedList<Integer> compute() {
		int length = arr.size();

		// Q2: rewrite the base case condition and body of this if statement,
		// so that you run:
		//
		// sequential merge sort for small inputs (the "base case")
		// by using SequentialMergeSort.mergeSort(..) 
		//
		// or run
		//
		// parallel merge sort in parallel for large inputs (the "recursive" case)
		
		// The following code tells the program to run a sequential merge sort if the length
		// of the array to be sorted is less than the threshold needed to run it in parallel.
		// (This is to avoid pointless forking of info between cores).
		if (length < threshold) {
			return SequentialMergeSort.mergeSort(arr);
		}

		else { // parallel case

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
			ParallelMergeSortThreshold resultLeft = new ParallelMergeSortThreshold(left, threshold);
			resultLeft.fork();

			/* populate the right array with values */
			index = 0;
			while (index < length - halfSize) {
				right.add(it.next());
				index++;
			}
			
			// The following code sets the right array to merge sort in the current
			// CPU core.
			ParallelMergeSortThreshold resultRight = new ParallelMergeSortThreshold(right, threshold);
			LinkedList<Integer> finalResultRight = resultRight.compute();
		
			// The following code puts the result from mergeLeft (which was computing
			// in another core) back into the core which mergeRight computed in.
			LinkedList<Integer> finalResultLeft = resultLeft.join();

			/* merge the sorted sub arrays */
			return SequentialMergeSort.merge(finalResultLeft, finalResultRight);
		}
	}

	/**
	 * Threshold based parallel merge sort
	 * 
	 * @param numbers     the input list
	 * @param threshold   when to switch from parallel divide-and-conquer to
	 *                    sequential divide-and-conquer
	 * @param parallelism how many threads to use in the ForkJoin workpool
	 * @return the sorted list
	 */
	public static LinkedList<Integer> parallelMergeSortThreshold(LinkedList<Integer> numbers, int threshold,
			int parallelism) {
		ForkJoinPool pool = new ForkJoinPool(parallelism);
		ParallelMergeSortThreshold mergeSortTask = new ParallelMergeSortThreshold(numbers, threshold);
		LinkedList<Integer> result = pool.invoke(mergeSortTask);
		return result;
	}

	/**
	 * Benchmarks threshold based parallel merge sort
	 */
	public static void main(String[] args) {
		/* generates a random list */
		LinkedList<Integer> numbers = SortingCommon.randomList(50000);

		/* gets the number of cores in this computer's CPU */
		int cpuCores = Runtime.getRuntime().availableProcessors();

		/*
		 * 1. prints the runtime for the parallel merge sort from Q1B.
		 * 
		 * 2. prints the runtime for the threshold based parallel merge sort for the
		 * implementation in Q2.
		 */
		for (int threads = 1; threads <= cpuCores; threads *= 2) {
			System.out.print("mergeSort\t no threshold\t\t");
			Benchmark.parallel(new ParallelMergeSort(numbers), threads);
			for (int threshold = 128; threshold <= 8192; threshold *= 4) {
				System.out.print("mergeSort\t threshold=" + threshold + "\t\t");
				Benchmark.parallel(new ParallelMergeSortThreshold(numbers, threshold), threads);
			}
		}
	}

}