package test;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mycompany.messageparsing.BucketSortSolver;
import com.mycompany.messageparsing.Util;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class BucketSortTest {
    /**private final int maxThreads = Runtime.getRuntime().availableProcessors();

    /**
     * Runs the algorithm sequentially and using activeMQ and checks whether both return a correctly sorted list
     */
    @Test
    public void listHasBeenSorted() {
        final int ARRAY_SIZE = 10_000;

        BucketSortSolver sorter = new BucketSortSolver();

        Random rng = new Random();
        List<Long> unsortedList = new ArrayList<>();
        for (int i = 0; i < ARRAY_SIZE; i++) {
            long value = Math.abs(rng.nextInt());
            unsortedList.add(value);
        }

        List<Long> activeMQSortedList = sorter.sortUsingActiveMQ(unsortedList);
        assertTrue(isSorted(activeMQSortedList));

        List<Long> sequentiallySortedList = sorter.sortSequentially(unsortedList);
        assertTrue(isSorted(sequentiallySortedList));
    }

    /**
     * Prints the time it takes to sort an array of 10000 elements in parallel and sequentially
     */
    @Test
    public void printTime() {
        final int ARRAY_SIZE = 10000;

        printTimeTaken(ARRAY_SIZE);
    }

    private boolean isSorted(List<Long> list) {
        for (int i = 1; i < list.size(); i++) {
            Long a = list.get(i - 1);
            Long b = list.get(i);
            if (a > b) {
                return false;
            }
        }
        return true;
    }

    private long measureTime(Util.Function function) {
        long before = System.currentTimeMillis();
        function.execute();
        long after = System.currentTimeMillis();

        return after - before;
    }

    private void printTimeTaken(int nElements) {
        BucketSortSolver sorter = new BucketSortSolver();
        Random rng = new Random();
        List<Long> unsortedList = new ArrayList<>();
        for (int i = 0; i < nElements; i++) {
            long value = Math.abs(rng.nextInt());
            unsortedList.add(value);
        }

        long activeMQTimeTaken = measureTime(() -> sorter.sortUsingActiveMQ(unsortedList));

        System.out.printf("ActiveMQ sorting took %d ms\n", activeMQTimeTaken);

        long sequentialTimeTaken = measureTime(() -> sorter.sortSequentially(unsortedList));
        System.out.printf("Sequential sorting took %d ms\n", sequentialTimeTaken);

        String faster = activeMQTimeTaken > sequentialTimeTaken ? "sequential" : "ActiveMQ";
        long difference = Math.abs(activeMQTimeTaken - sequentialTimeTaken);
        System.out.printf("%s was %d ms faster\n", faster, difference);
    }

    //////////////////////////////////////////  DATA COLLECTION  ///////////////////////////////////////////////////////
    ///  Underneath follow the tests we ran to gather the data shown in the graphs.                                  ///
    ///  Running them as-is on a sub-par computer may result in lag or a crash due to large numbers and long runtime ///
    ///  We recommend adjusting their values accordingly beforehand if you wish to run them yourself.                ///
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

//    @Test
//    public void nThreadsBenchmark() {
//        int[] nElementsArray = new int[]{ 100_000, 1_000_000, 5_000_000, 10_000_000, 25_000_000 };
//
//        for (int nElements : nElementsArray) {
//            for (int threads = 1; threads <= maxThreads; threads++) {
//                System.out.println();
//                System.out.printf("%d thread(s), %d elements:\n", threads, nElements);
//                printTimeTaken(threads, nElements);
//            }
//        }
//    }

//    @Test
//    public void nElementsBenchmark() {
//        int nElements = 10_000;
//
//        for (int i = 0; i < 100; i++) {
//            int currentNElements = (i * i) * nElements;
//            System.out.println("i: " + i + " # of elements: " + currentNElements);
//            printTimeTaken(maxThreads, currentNElements);
//        }
//    }
}