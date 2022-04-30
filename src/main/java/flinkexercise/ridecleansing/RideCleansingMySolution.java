package flinkexercise.ridecleansing;

import flinkexercise.common.datatypes.TaxiRide;
import flinkexercise.common.sources.TaxiRideGenerator;
import flinkexercise.common.utils.GeoUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.reflect.internal.Trees;

public class RideCleansingMySolution {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<TaxiRide> rideDataStream = env.addSource(new TaxiRideGenerator());

        DataStream<TaxiRide> NYCDataStream = rideDataStream.filter(new FilterFunction<TaxiRide>() {
            @Override
            public boolean filter(TaxiRide taxiRide) throws Exception {
                return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat) &&
                        GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);
            }
        });

        NYCDataStream.print();
        env.execute("RideCleansing");
    }
}
