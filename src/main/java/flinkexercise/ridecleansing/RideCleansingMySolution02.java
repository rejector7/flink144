package flinkexercise.ridecleansing;

import flinkexercise.common.datatypes.TaxiRide;
import flinkexercise.common.sources.TaxiRideGenerator;
import flinkexercise.common.utils.GeoUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class RideCleansingMySolution02 {

    private final SourceFunction<TaxiRide> source;
    private final SinkFunction<TaxiRide> sink;

    public RideCleansingMySolution02(SourceFunction<TaxiRide> source, SinkFunction<TaxiRide> sink){
        this.source = source;
        this.sink = sink;
    }

    public JobExecutionResult execute() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(source).filter(new NYCFilter()).addSink(sink);

        return env.execute("RS02");
    }

    public static class NYCFilter implements FilterFunction<TaxiRide> {

        @Override
        public boolean filter(TaxiRide taxiRide) {
            return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat)
                    && GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);
        }
    }



    public static void main(String[] args) throws Exception {
        RideCleansingMySolution02 solution = new RideCleansingMySolution02(new TaxiRideGenerator(), new PrintSinkFunction<>());

        solution.execute();

    }
}
