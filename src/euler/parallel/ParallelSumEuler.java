package euler.parallel;

import java.util.ArrayList;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;

import euler.sequential.SumEuler;
import utils.Benchmark;

/*
 * Sum Euler results
 * ~~~~~~~~~~~~~~~~~~
 *
 * Parallelisation results: TODO
 * - 1 thread: 
 * - 2 threads:
 * - <insert more lines as appropriate>
 * 
 */

class ParSumEuler extends RecursiveTask<Long> {
	Long lower, upper;

	public ParSumEuler(long lower, long upper) {
		this.lower = lower;
		this.upper = upper;
	}

	@Override
	protected Long compute() {
		ArrayList<EulerTask> tasks = new ArrayList<EulerTask>(upper.intValue());
		for (long i = lower; i <= upper; i++) {
			EulerTask task = new EulerTask(i);
			task.fork();
			tasks.add(task);
		}

		long sum = 0;
		int taskCount = 0;
		for (long i = lower; i <= upper; i++) {
			sum = sum + tasks.get(taskCount).join();
			taskCount++;
		}
		return sum;
	}
}

public class ParallelSumEuler {

	public static Long parallelSumEuler(Long lower, Long upper, int parallelism) {
		ForkJoinPool pool = new ForkJoinPool(parallelism);
		ParSumEuler sumEulerTask = new ParSumEuler(lower, upper);
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
			Benchmark.parallel(new ParSumEuler(lower, upper), threads);
		}
	}
}
