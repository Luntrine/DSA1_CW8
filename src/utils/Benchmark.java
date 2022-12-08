package utils;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;

public class Benchmark {

	public static <T> void parallel(RecursiveTask<T> task, int parallelism) {
		// time before
		long startTime = System.currentTimeMillis();

		// run the parallel benchmark
		ForkJoinPool pool = new ForkJoinPool(parallelism);
		pool.invoke(task);

		// time after
		long stopTime = System.currentTimeMillis();
		
		// how long it took
		long time = stopTime - startTime;
		System.out.println(parallelism + " threads\truntime=" + time + "ms\t" + pool.getStealCount() + " steals");
	}
	
}
