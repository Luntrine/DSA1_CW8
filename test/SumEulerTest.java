import static org.junit.Assert.assertEquals;

import org.junit.Test;

import euler.parallel.ParallelSumEuler;
import euler.parallel.ParallelSumEulerChunked;
import euler.sequential.SumEuler;

/*
 * Uncomment these tests if you are completing the
 * parallel Sum Euler optional task.
 */

public class SumEulerTest {

//	@Test
//	public void sumEulerSeq100() {
//		assertEquals(3043, SumEuler.sumEuler(0, 100));
//	}
//
//	@Test
//	public void sumEulerSeq1000() {
//		assertEquals(304191, SumEuler.sumEuler(0, 1000));
//	}
//
//	@Test
//	public void sumEulerPar100() {
//		Long result = ParallelSumEuler.parallelSumEuler((long) 0, (long) 100, 4);
//		assertEquals(3043, result.longValue());
//	}
//
//	@Test
//	public void sumEulerPar1000() {
//		Long result = ParallelSumEuler.parallelSumEuler((long) 0, (long) 1000, 4);
//		assertEquals(304191, result.longValue());
//	}
//
//	@Test
//	public void sumEulerParThresh100() {
//		Long result = ParallelSumEuler.parallelSumEuler((long) 0, (long) 100, 4);
//		assertEquals(3043, result.longValue());
//	}
//
//	@Test
//	public void sumEulerParChunked1000() {
//		Long result = ParallelSumEulerChunked.parallelSumEulerChunked((long) 0, (long) 1000, 50, 4);
//		assertEquals(304191, result.longValue());
//	}
//
//	/**
//	 * Ensures that sequential sum euler results match the parallel sum euler
//	 * results. This is assuming the sequential version is the ground truth.
//	 */
//	@Test
//	public void sumEulerSameParallel() {
//		Long sumEulerSeq10 = SumEuler.sumEuler(1, 10);
//		Long sumEulerPar10 = ParallelSumEuler.parallelSumEuler((long) 1, (long) 10, 4);
//		Long sumEulerParChunked10 = ParallelSumEulerChunked.parallelSumEulerChunked((long) 1, (long) 10, 2, 4);
//
//		assertEquals(sumEulerSeq10, sumEulerPar10);
//		assertEquals(sumEulerSeq10, sumEulerParChunked10);
//
//		Long sumEulerSeq100 = SumEuler.sumEuler(1, 100);
//		Long sumEulerPar100 = ParallelSumEuler.parallelSumEuler((long) 1, (long) 100, 4);
//		Long sumEulerParChunked100 = ParallelSumEulerChunked.parallelSumEulerChunked((long) 1, (long) 100, 10, 4);
//
//		assertEquals(sumEulerSeq100, sumEulerPar100);
//		assertEquals(sumEulerSeq100, sumEulerParChunked100);
//
//		Long sumEulerSeq1000 = SumEuler.sumEuler(1, 1000);
//		Long sumEulerPar1000 = ParallelSumEuler.parallelSumEuler((long) 1, (long) 1000, 4);
//		Long sumEulerParChunked1000 = ParallelSumEulerChunked.parallelSumEulerChunked((long) 1, (long) 1000, 100, 4);
//
//		assertEquals(sumEulerSeq1000, sumEulerPar1000);
//		assertEquals(sumEulerSeq1000, sumEulerParChunked1000);
//	}
}
