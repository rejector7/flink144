package flinkexercise.ridesandfares;

import flinkexercise.common.datatypes.RideAndFare;
import flinkexercise.common.datatypes.TaxiFare;
import flinkexercise.common.datatypes.TaxiRide;
import flinkexercise.testing.ParallelTestSource;
import flinkexercise.testing.TestSink;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class RidesAndFaresMyIntegrationTest extends RidesAndFaresTestBase{

    @Test
    public void testSeveralRidesAndFaresMixedTogether() throws Exception {
        final TaxiRide ride1 = testRide(1);
        final TaxiFare fare1 = testFare(1);

        final TaxiRide ride2 = testRide(2);
        final TaxiFare fare2 = testFare(2);

        final TaxiRide ride3 = testRide(3);
        final TaxiFare fare3 = testFare(3);

        final TaxiRide ride4 = testRide(4);
        final TaxiFare fare4 = testFare(4);

        ParallelTestSource<TaxiRide> rideSource = new ParallelTestSource<>(ride1, ride2, ride3, ride4);
        ParallelTestSource<TaxiFare> fareSource = new ParallelTestSource<>(fare3, fare4, fare2, fare1);

        TestSink<RideAndFare> sink = new TestSink<>();
        RidesAndFaresMySolution solution = new RidesAndFaresMySolution(rideSource, fareSource, sink);
        JobExecutionResult result = solution.execute();
        assertThat(sink.getResults(result)).containsExactlyInAnyOrder(
                new RideAndFare(ride1, fare1),
                new RideAndFare(ride2, fare2),
                new RideAndFare(ride3, fare3),
                new RideAndFare(ride4, fare4)
        );
    }
}
