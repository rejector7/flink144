package flinkexercise.ridesandfares;

import flinkexercise.common.datatypes.RideAndFare;
import flinkexercise.common.datatypes.TaxiFare;
import flinkexercise.common.datatypes.TaxiRide;
import flinkexercise.common.sources.TaxiFareGenerator;
import flinkexercise.common.sources.TaxiRideGenerator;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import scala.Enumeration;

import javax.xml.crypto.Data;

public class RidesAndFaresMySolution {
    private final SourceFunction<TaxiRide> rideSourceFunction;
    private final SourceFunction<TaxiFare> fareSourceFunction;
    private final SinkFunction<RideAndFare> sinkFunction;


    public RidesAndFaresMySolution(SourceFunction<TaxiRide> rideSourceFunction,
                                   SourceFunction<TaxiFare> fareSourceFunction,
                                   SinkFunction<RideAndFare> sinkFunction){
        this.rideSourceFunction = rideSourceFunction;
        this.fareSourceFunction = fareSourceFunction;
        this.sinkFunction = sinkFunction;
    }

    public JobExecutionResult execute() throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<TaxiRide> rideDataStream = env.addSource(rideSourceFunction)
                .filter(taxiRide -> taxiRide.isStart)
                .keyBy(taxiRide -> taxiRide.rideId);

        DataStream<TaxiFare> fareDataStream = env.addSource(fareSourceFunction)
                .keyBy(taxiFare -> taxiFare.rideId);

        rideDataStream.connect(fareDataStream)
                .flatMap(new EnrichmentCoFunction())
                .addSink(sinkFunction);

        return env.execute("ridesAndFares");
    }

    public static void main(String[] args) throws Exception {
        RidesAndFaresMySolution solution = new RidesAndFaresMySolution(
                new TaxiRideGenerator(),
                new TaxiFareGenerator(),
                new PrintSinkFunction<>()
        );

        solution.execute();
    }

    public static class EnrichmentCoFunction extends RichCoFlatMapFunction<TaxiRide, TaxiFare, RideAndFare>{
        private ValueState<TaxiRide> rideValueState;
        private ValueState<TaxiFare> fareValueState;

        @Override
        public void open(Configuration config) throws Exception {
            rideValueState = getRuntimeContext().getState(new ValueStateDescriptor<TaxiRide>("rideValueState", TaxiRide.class));

            fareValueState = getRuntimeContext().getState(
                    new ValueStateDescriptor<TaxiFare>("fareValueState", TaxiFare.class)
            );

        }

        @Override
        public void flatMap1(TaxiRide ride, Collector<RideAndFare> out) throws Exception {
            TaxiFare fare = fareValueState.value();
            if(fare != null){
                fareValueState.clear();
                out.collect(new RideAndFare(ride, fare));
            }else {
                rideValueState.update(ride);
            }
        }

        @Override
        public void flatMap2(TaxiFare fare, Collector<RideAndFare> out) throws Exception {
            TaxiRide ride = rideValueState.value();
            if(ride != null){
                rideValueState.clear();
                out.collect(new RideAndFare(ride, fare));
            } else {
                fareValueState.update(fare);
            }
        }
    }
}
