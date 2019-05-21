package com.netflix.conductor.aurora.sql;


import java.util.function.Supplier;

/**
 * Functional class to support the lazy execution of a String result.
 */
public class LazyToString {
    private final Supplier<String> supplier;

    /**
     * @param supplier Supplier to execute when {@link #toString()} is called.
     */
    public LazyToString(Supplier<String> supplier) {
        this.supplier = supplier;
    }

    @Override
    public String toString() {
        return supplier.get();
    }
}
