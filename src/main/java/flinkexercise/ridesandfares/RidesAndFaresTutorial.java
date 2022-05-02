package flinkexercise.ridesandfares;

import flinkexercise.common.datatypes.TaxiRide;
import flinkexercise.common.utils.GeoUtils;
import flinkexercise.ridecleansing.RideCleansingMySolution02;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

public class RidesAndFaresTutorial {
    public static class FlatMapNYCEnrichment implements FlatMapFunction<TaxiRide, EnrichedRide> {


        @Override
        public void flatMap(TaxiRide taxiRide, Collector<EnrichedRide> collector) throws Exception {
            if(new RideCleansingMySolution02.NYCFilter().filter(taxiRide)){
                collector.collect(new EnrichedRide(taxiRide));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> control = env
                .fromElements("DROP", "IGNORE",
                        "1", "2", "3", "4", "5", "6", "7", "8", "9", "10",
                        "11", "12", "13", "14", "15", "16")
                .keyBy(x -> x);

        DataStream<String> streamOfWords = env
                .fromElements(
                        "1", "2", "3", "4", "5", "6", "7", "8", "9", "10",
                        "11", "12", "13", "14", "15", "16",
                        "Apache", "DROP", "Flink", "IGNORE")
                .keyBy(x -> x);

        control
                .connect(streamOfWords)
                .flatMap(new ControlFunction())
                .print();

        env.execute();
    }

    public static class ControlFunction extends RichCoFlatMapFunction<String, String, String> {
        private ValueState<Boolean> blocked;

        @Override
        public void open(Configuration config) {
            blocked = getRuntimeContext()
                    .getState(new ValueStateDescriptor<>("blocked", Boolean.class));
        }

        @Override
        public void flatMap1(String control_value, Collector<String> out) throws Exception {
            blocked.update(Boolean.TRUE);
        }

        @Override
        public void flatMap2(String data_value, Collector<String> out) throws Exception {
            if (blocked.value() == null) {
                out.collect(data_value);
            }
        }
    }
}
