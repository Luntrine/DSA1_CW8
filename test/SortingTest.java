import static org.junit.Assert.*;

import java.util.LinkedList;

import org.junit.Test;

import sort.parallel.ParallelMergeSort;
import sort.parallel.ParallelMergeSortThreshold;
import sort.sequential.SequentialMergeSort;
import sort.sequential.SortingCommon;

public class SortingTest {

	@Test
	public void testMergeSortOrdered() {
		LinkedList<Integer> numbers = new LinkedList<Integer>();
		numbers.add(1);
		numbers.add(2);
		numbers.add(3);
		numbers.add(4);
		numbers.add(5);
		LinkedList<Integer> result = ParallelMergeSort.parallelMergeSort(numbers, 4);
		assertTrue("length incorrect", result.size() == 5);
		assertTrue("not sorted", SortingCommon.isSorted(result));
	}

	@Test
	public void testMergeSortThreshOrdered() {
		LinkedList<Integer> numbers = new LinkedList<Integer>();
		numbers.add(1);
		numbers.add(2);
		numbers.add(3);
		numbers.add(4);
		numbers.add(5);
		LinkedList<Integer> result = ParallelMergeSortThreshold.parallelMergeSortThreshold(numbers, 10, 4);
		assertTrue("length incorrect", result.size() == 5);
		assertTrue("not sorted", SortingCommon.isSorted(result));
	}

	@Test
	public void testMergeSortRandom() {
		LinkedList<Integer> numbers = new LinkedList<Integer>();
		numbers.add(1);
		numbers.add(5);
		numbers.add(2);
		numbers.add(9);
		numbers.add(5);
		LinkedList<Integer> result = ParallelMergeSort.parallelMergeSort(numbers, 4);
		assertTrue("length incorrect", result.size() == 5);
		assertTrue("not sorted", SortingCommon.isSorted(result));
	}

	@Test
	public void testMergeSortThreshRandom() {
		LinkedList<Integer> numbers = new LinkedList<Integer>();
		numbers.add(1);
		numbers.add(5);
		numbers.add(2);
		numbers.add(9);
		numbers.add(5);
		LinkedList<Integer> result = ParallelMergeSortThreshold.parallelMergeSortThreshold(numbers, 10, 4);
		assertTrue("length incorrect", result.size() == 5);
		assertTrue("not sorted", SortingCommon.isSorted(result));
	}

	/**
	 * Ensures that sequential merge sort results match the parallel merge sort
	 * results with a random generated list of integers. This is assuming the
	 * sequential merge sort as the ground truth.
	 */
	@Test
	public void parallelMergeSortTest() {
		LinkedList<Integer> list = SortingCommon.randomList(1000);
		
		LinkedList<Integer> sortSequential = SequentialMergeSort.mergeSort(list);
		LinkedList<Integer> sortParallel = ParallelMergeSort.parallelMergeSort(list, 2);
		LinkedList<Integer> sortParallelThresh = ParallelMergeSortThreshold.parallelMergeSortThreshold(list, 10, 2);

		/* check that sequential merge sort has worked */
		assertTrue(SortingCommon.isSorted(SequentialMergeSort.mergeSort(list)));
		
		/* now compare the two parallel implementations with the sequential result */
		assertEquals(sortSequential, sortParallel);
		assertEquals(sortSequential, sortParallelThresh);
	}
}
