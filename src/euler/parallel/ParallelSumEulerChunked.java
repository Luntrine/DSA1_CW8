package euler.parallel;

import java.util.ArrayList;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;

import euler.sequential.SumEuler;
import utils.Benchmark;

/*
 * Chunked based Sum Euler results
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 *
 * Parameters of the shortest runtime: TODO
 * - how many threads:
 * - chunk size:
 */

class ParSumEulerChunked extends RecursiveTask<Long> {
	Long lower, upper;
	int chunkSize;

	public ParSumEulerChunked(long lower, long upper, int chunkSize) {
		this.lower = lower;
		this.upper = upper;
		this.chunkSize = chunkSize;
	}

	@Override
	protected Long compute() {
		ArrayList<SumEulerTask> tasks = new ArrayList<SumEulerTask>(upper.intValue());
		for (long i = lower; i <= upper; i += chunkSize) {
			// TODO Step 1: create a EumEulerTask
			
			// TODO Step 2: fork the task
			
			// TODO Step 3: add it to the list of tasks

		}

		long sum = 0;
		int taskCount = 0;
		for (long i = lower; i <= upper; i += chunkSize) {
			// TODO Step 4: wait for the result from next task in the list
			//              from position taskCount in the task list.
			
			// TODO Step 5: add it to the 'sum' which keeps track of
			//              summing Euler computations.
			
			// TODO Step 6: Increment the task count.

		}
		return sum;
	}
}

public class ParallelSumEulerChunked {

	public static Long parallelSumEulerChunked(Long lower, Long upper, int chunkSize, int parallelism) {
		ForkJoinPool pool = new ForkJoinPool(parallelism);
		ParSumEulerChunked sumEulerTask = new ParSumEulerChunked(lower, upper, chunkSize);
		Long result = pool.invoke(sumEulerTask);
		return result;
	}

	public static void main(String[] args) {
		final int lower = 1;
		final int upper = 10000;
		System.out.print("sequential\t");
		SumEuler.benchmark(lower, upper);
		int cpuCores = Runtime.getRuntime().availableProcessors();
		for (int threads = 1; threads <= cpuCores; threads *= 2) {
			System.out.print("no chunking\t");
			Benchmark.parallel(new ParSumEuler(lower, upper), threads);
			for (int chunkSize = 5; chunkSize <= 20; chunkSize += 5) {
				System.out.print("chunkSize=" + chunkSize + "\t");
				Benchmark.parallel(new ParSumEulerChunked(lower, upper, chunkSize), threads);
			}
		}
	}
}
