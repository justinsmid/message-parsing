package com.mycompany.messageparsing;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Util {
    public static Long findMax(List<Long> list) {
        Long max = Long.MIN_VALUE;
        for (Long element : list) {
            if (element > max) {
                max = element;
            }
        }

        return max;
    }

    public static List<Long> randomListOfSize(int size) {
        Random rng = new Random();
        List<Long> list = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            long value = Math.abs(rng.nextInt());
            list.add(value);
        }
        return list;
    }

    public interface Function {
        public void execute();
    }
}
