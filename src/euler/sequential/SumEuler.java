package euler.sequential;

public class SumEuler {

	private static long hcf(long x, long y) {
		long t;
		while (y != 0) {
			t = x % y;
			x = y;
			y = t;
		}
		return x;
	}

	private static boolean relprime(long x, long y) {
		return hcf(x, y) == 1;
	}

	public static long euler(long n) {
		long length = 0;
		for (long i = 1; i < n; i++)
			if (relprime(n, i))
				length++;
		return length;
	}
	
	/** Sequential Sum Euler
	 * @param lower lowest euler value to compute
	 * @param upper highest euler value to compute
	 * @return the sum of all euler values between lower and upper
	 */
	public static long sumEuler(long lower, long upper)
	{
	  long sum = 0;
	  for (long i = lower; i <= upper; i++) {
	    sum = sum + euler(i);
	  }
	  return sum;
	}
	
	public static <T> void benchmark(long lower, long upper) {
		// time before
		long startTime = System.currentTimeMillis();

		// run the parallel benchmark
		long result = sumEuler(lower, upper);
		
		// time after
		long stopTime = System.currentTimeMillis();
		
		// how long it took
		long time = stopTime - startTime;
		System.out.println("sumEuler(" + lower + "," + upper + ")=" + result + " : " + time + "ms");
	}
	
	public static void main(String[] args) {
		benchmark(0,100);
		benchmark(0,1000);
		benchmark(0,10000);
	}

}
