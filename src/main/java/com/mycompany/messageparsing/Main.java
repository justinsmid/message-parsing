package com.mycompany.messageparsing;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Main {
    public static void main(String[] args) {
        final int AMOUNT_OF_VALUES = 10_000;

        BucketSortSolver solver = new BucketSortSolver();

        Random rng = new Random();
        List<Long> unsortedList = new ArrayList<>();
        for (int i = 0; i < AMOUNT_OF_VALUES; i++) {
            long value = Math.abs(rng.nextInt());
            unsortedList.add(value);
        }

        solver.solve(unsortedList);
    }
}
