package flinkexercise.ridesandfares;

import flinkexercise.common.datatypes.TaxiRide;
import flinkexercise.common.utils.GeoUtils;
import flinkexercise.ridecleansing.RideCleansingMySolution02;
import org.apache.flink.api.common.functions.FlatMapFunction;
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
}
