package flinkexercise.ridesandfares;

import flinkexercise.common.datatypes.TaxiFare;
import flinkexercise.common.datatypes.TaxiRide;

import java.time.Instant;

public class RidesAndFaresTestBase {
    public static TaxiRide testRide(long rideId){
        return new TaxiRide(rideId, true, Instant.EPOCH, 0F, 0F, 0F, 0F, (short) 1, 0, rideId);
    }

    public static TaxiFare testFare(long rideId){
        return new TaxiFare(rideId, 0, rideId, Instant.EPOCH, "", 0F, 0F, 0F);
    }
}
