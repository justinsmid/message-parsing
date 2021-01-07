package com.mycompany.messageparsing;

import java.util.List;

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

    public interface Function {
        public void execute();
    }
}
