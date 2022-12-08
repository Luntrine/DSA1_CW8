package euler.parallel;

import java.util.concurrent.RecursiveTask;

import euler.sequential.SumEuler;

public class SumEulerTask extends RecursiveTask<Long> {
	final long lower, upper;

	SumEulerTask(long lower, long upper) {
		this.lower = lower;
		this.upper = upper;
	}

	@Override
	protected Long compute() {
		return SumEuler.sumEuler(lower, upper);
	}
}