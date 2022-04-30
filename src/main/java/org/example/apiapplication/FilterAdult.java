package org.example.apiapplication;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FilterAdult {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Person> personDataStream = env.fromElements(
                new Person(10, "abc"),
                new Person(15, "def"),
                new Person(21, "ghi"),
                new Person(25, "jkl")
        );

        DataStream<Person> adults = personDataStream.filter(new FilterFunction<Person>() {
            @Override
            public boolean filter(Person person) throws Exception {
                return person.age >= 18;
            }
        });

        adults.print();
        env.execute("filterAdult");

    }
}
