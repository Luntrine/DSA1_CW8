package euler.parallel;

import java.util.concurrent.RecursiveTask;

import euler.sequential.SumEuler;

public class EulerTask extends RecursiveTask<Long> {
	final long n;

	EulerTask(long n) {
		this.n = n;
	}

	@Override
	protected Long compute() {
		return SumEuler.euler(n);
	}
}
