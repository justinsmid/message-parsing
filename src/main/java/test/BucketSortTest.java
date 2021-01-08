package test;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mycompany.messageparsing.BucketSortSolver;
import com.mycompany.messageparsing.Util;
import org.junit.jupiter.api.Test;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;

public class BucketSortTest {
    private final int DEFAULT_ARRAY_SIZE = 100_000;

    /**
     * Runs the algorithm sequentially and using activeMQ and checks whether the sorted lists are valid.
     * The lists are valid if they are sorted and still contain the same elements as the unsorted list.
     */
    @Test
    public void sortedListIsValid() {
        BucketSortSolver sorter = new BucketSortSolver();

        List<Long> unsortedList = Util.randomListOfSize(DEFAULT_ARRAY_SIZE);

        List<Long> activeMQSortedList = sorter.sortUsingActiveMQ(unsortedList);
        assertTrue(isSorted(activeMQSortedList));

        List<Long> sequentiallySortedList = sorter.sortSequentially(unsortedList);
        assertTrue(isSorted(sequentiallySortedList));

        assertTrue(CollectionUtils.isEqualCollection(unsortedList, activeMQSortedList));
        assertTrue(CollectionUtils.isEqualCollection(unsortedList, sequentiallySortedList));
        assertTrue(CollectionUtils.isEqualCollection(activeMQSortedList, sequentiallySortedList));
    }

    /**
     * Prints the time it takes to sort an array of 100000 elements in parallel and sequentially
     */
    @Test
    public void printTime() {
        printTimeTaken(DEFAULT_ARRAY_SIZE);
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

        List<Long> unsortedList = Util.randomListOfSize(nElements);

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
//    public void nElementsBenchmark() {
//        final int[] nElementsArray = new int[]{
//                10_000, 25_000, 50_000, 100_000, 250_000, 500_000, 1_000_000, 2_500_000, 5_000_000, 7_000_000
//        };
//
//        for (int nElements : nElementsArray) {
//            System.out.printf("%d elements:\n", nElements);
//            printTimeTaken(nElements);
//            System.out.println("");
//        }
//    }
}