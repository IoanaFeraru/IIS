package org.datasource.jdbc.jmh;

import org.datasource.jdbc.views.customerdetails.CustomerDetailsViewBuilder;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.stereotype.Component;

@SpringBootTest
public class Test_SQLJDBCView_JMH_SBT {
    //
    @Component
    @State(Scope.Thread)
    public static class BenchmarkState {
        @Autowired
        CustomerDetailsViewBuilder customerDetailsViewBuilder; // Will be injected by Spring

        // Initialize Spring context manually before benchmarks run
        @Setup(Level.Trial)
        public void setup() {
            // No-op if Spring context is already injected
        }
    }
    //

    @Benchmark
    public void benchmarkMethod(BenchmarkState state) {
        state.customerDetailsViewBuilder.build().getViewList();
    }

    @Test
    public void runBenchmarks() throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(this.getClass().getSimpleName())
                .forks(0) // Important: no fork to keep Spring context alive
                .build();
        new Runner(opt).run();
    }
}
