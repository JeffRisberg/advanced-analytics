package com.incra.timing;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jeff on 10/24/15.
 */
public class FilterJava {
    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        System.out.println("start");

        new JavaFilterExample();
        new JavaFilterExample();
        new JavaFilterExample();
        new JavaFilterExample();
        new JavaFilterExample();

        long elapsed = System.currentTimeMillis() - start;
        System.out.println("Total " + elapsed);
    }
}

class JavaFilterExample {
    public JavaFilterExample() {
        long start = System.currentTimeMillis();

        for (int i = 0; i < 10000; i++) {
            String[] states = {"NY", "CA", "NJ", "OH", "OK", "MA", "TX", "MN", "ORE", "FL", "CT", "PA", "WA", "VA", "ME", "VT", "NH", "NV"};

            List<String> validStates = new ArrayList<String>();
            for (String state : states) {
                if (state.length() == 2) validStates.add(state);
            }
            List<String> hawaii = new ArrayList<String>();
            for (String state : validStates) {
                if (state.equals("HI")) hawaii.add(state);
            }
            //System.out.println(validStates.size());
        }
        long elapsed = System.currentTimeMillis() - start;
        System.out.println(elapsed);
    }
}