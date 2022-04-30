package flinkexercise.ridecleansing;

import flinkexercise.common.datatypes.TaxiRide;
import org.apache.flink.api.common.functions.FilterFunction;
import org.junit.Test;

import java.time.Instant;

import static org.junit.Assert.*;

public class RideCleansingMySolution02Test {

    FilterFunction<TaxiRide> filterFunction = new RideCleansingExercise.NYCFilter();

    public static TaxiRide testRide(float startLon, float startLat, float endLon, float endLat){
        return new TaxiRide(1L, true, Instant.EPOCH, startLon, startLat, endLon, endLat, (short) 0, 1L, 1L);
    }

    @Test
    public void testRideThatStartsAndEndsInNYC() throws Exception {
        TaxiRide testRide = testRide(-73.9947F, 40.750626F, -73.9947F, 40.750626F);
        assertTrue(filterFunction.filter(testRide));
    }

    @Test
    public void testRideThatStartsOutsideNYC() throws Exception{
        TaxiRide testRide = testRide(0, 90, -73.9947F, 40.750626F);
        assertFalse(filterFunction.filter(testRide));
    }
}