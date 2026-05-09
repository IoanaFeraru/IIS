package org.datasource.jdbc.jmh;

import org.datasource.jdbc.views.customerdetails.CustomerDetailsViewBuilder;
import org.openjdk.jmh.annotations.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.concurrent.TimeUnit;

@SpringBootTest
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class Test_SQLJDBCView_JMH extends AbstractBenchmark {
    //

    public static CustomerDetailsViewBuilder customerDetailsViewBuilder;

    @Autowired
    public static void setCustomerDetailsViewBuilder(CustomerDetailsViewBuilder customerDetailsViewBuilder) {
        Test_SQLJDBCView_JMH.customerDetailsViewBuilder = customerDetailsViewBuilder;
    }

    //
    @Benchmark
    public void test_CustomerDetailsView(){
        System.out.println("customerDetailsViewBuilder is null? " + customerDetailsViewBuilder);
        //List<CustomerDetailsView> viewList = customerDetailsViewBuilder.build().getViewList();
    }
}


/*
https://gist.github.com/msievers/ce80d343fc15c44bea6cbb741dde7e45

@Benchmark
    @Warmup(iterations = 5)
    @Measurement(iterations = 10)
    @Fork(1)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)


 Options options = new OptionsBuilder()
                .include(this.getClass().getSimpleName()) // Include benchmarks in this class only
                .warmupIterations(3)                       // JVM warm-up iterations
                .measurementIterations(3)                  // Measurement iterations
                .forks(0)                                 // No forking to keep Spring context alive
                .threads(1)                               // Single thread for benchmarking
                .shouldDoGC(true)
                .shouldFailOnError(true)
                .build();
 */