package ex;

import ex.deserialization.objects.Flight;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;

import org.apache.parquet.filter2.predicate.Operators.Column;

public class AirportInfoImpl implements AirportInfo {

    /**
     * Usage of some example operations.
     *
     * @param flights dataframe of flights
     */
    public void sparkExample(Dataset<Row> flights) {
        System.out.println("Example printSchema");
        flights.printSchema();

        System.out.println("Example select and filter");
        // operations on a dataframe or rdd do not modify it, but return a new one
        Dataset<Row> selectAirlineDisplayCode = flights.select("flight.operatingAirline.iataCode", "flight.aircraftType.icaoCode").filter(r -> !r.anyNull());
        System.out.println("selectAirlineDisplayCode.count(): " + selectAirlineDisplayCode.count());
        selectAirlineDisplayCode.show(false);

        System.out.println("Example groupBy and count");
        Dataset<Row> countOfAirlines = selectAirlineDisplayCode.groupBy("iataCode").count();
        countOfAirlines.show(false);

        System.out.println("Example where");
        Dataset<Row> selectOnlyLufthansa = selectAirlineDisplayCode.where("iataCode = 'LH'");
        selectOnlyLufthansa.show(false);

        System.out.println("Example map to String");
        Dataset<String> onlyAircraftIcaoCodeAsString = selectOnlyLufthansa.map(r -> r.getString(1), Encoders.STRING());
        onlyAircraftIcaoCodeAsString.show(false);

        System.out.println("Example mapToPair and reduceByKey");
        JavaRDD<Row> rdd = selectOnlyLufthansa.toJavaRDD();
        JavaPairRDD<String, Long> paired = rdd.mapToPair(r -> Tuple2.apply(r.getString(1), 1L));
        JavaPairRDD<String, Long> reducedByKey = paired.reduceByKey((a, b) -> a + b);
        reducedByKey.take(20).forEach(t -> System.out.println(t._1() + " " + t._2()));
    }

    /**
     * Task 1
     * Return a dataframe, in which every row contains the destination airport and its count over all available data
     * of departing flights, sorted by count in descending order.
     * The column names are (in this order):
     * arrivalAirport | count
     * Remove entries that do not contain an arrivalAirport member (null or empty).
     * Use the given Dataframe.
     *
     * @param departingFlights Dataframe containing the rows of departing flights
     * @return a dataframe containing statistics of the most common destinations
     */
    @Override
    public Dataset<Row> mostCommonDestinations(Dataset<Row> departingFlights) {
        // TODO: Implement
        Dataset<Row> statsDepartingFlightsForEachArrival =
            departingFlights.select("flight.arrivalAirport")
            .filter(r -> !r.anyNull()).filter("arrivalAirport != ''")
            .groupBy("arrivalAirport").count().sort(col("count").desc());
        return statsDepartingFlightsForEachArrival;
    }

    /**
     * Task 2
     * Return a dataframe, in which every row contains a gate and the amount at which that gate was used for flights to
     * Berlin ("count"), sorted by count in descending order. Do not include gates with 0 flights to Berlin.
     * The column names are (in this order):
     * gate | count
     * Remove entries that do not contain a gate member (null or empty).
     * Use the given Dataframe.
     *
     * @param departureFlights Dataframe containing the rows of departing flights
     * @return dataframe with statistics about flights to Berlin per gate
     */
    @Override
    public Dataset<Row> gatesWithFlightsToBerlin(Dataset<Row> departureFlights) {
        // TODO: Implement
        Dataset<Row> berlinArrivalFlights = departureFlights.where("flight.arrivalAirport = 'TXL'")
            .select(explode(col("flight.departure.gates.gate"))).withColumnRenamed("col", "gate")
            .filter(r -> !r.anyNull()).filter("gate != ''").groupBy("gate").count()
            .sort(col("count").desc());
        return berlinArrivalFlights;
    }

    /**
     * Task 3
     * Return a JavaPairRDD with String keys and Long values, containing count of flights per aircraft on the given
     * originDate. The String keys are the modelNames of each aircraft and their Long value is the amount of flights for
     * that modelName at the given originDate. Do not include aircrafts with 0 flights.
     * Remove entries that do not contain a modelName member (null or empty).
     * The date string is of the form 'YYYY-MM-DD'.
     * Use the given dataframe.
     *
     * @param flights    Dataframe containing the rows of flights
     * @param originDate the date to find the most used aircraft for
     * @return tuple containing the modelName of the aircraft and its total number
     */
    @Override
    public JavaPairRDD<String, Long> aircraftCountOnDate(Dataset<Row> flights, String originDate) {
        // TODO: Implement
        Dataset<Row> queriedFlights = flights
            .where("flight.originDate = '" + originDate +"'")
            .select("flight.aircraftType.modelName")
            .filter(r -> !r.anyNull())
            .filter("modelName != ''");
        JavaRDD<Row> rdd = queriedFlights.toJavaRDD();
        JavaPairRDD<String, Long> paired = rdd.mapToPair(r -> Tuple2.apply(r.getString(0), 1L));
        return paired.reduceByKey((a, b) -> a + b);
    }

    /**
     * Task 4
     * Returns the date string of the day at which Ryanair had a strike in the given Dataframe.
     * The returned string is of the form 'YYYY-MM-DD'.
     * Hint: There were strikes at two days in the given period of time. Both are accepted as valid results.
     *
     * @param flights Dataframe containing the rows of flights
     * @return day of strike
     */
    @Override
    public String ryanairStrike(Dataset<Row> flights) {
        // TODO: Implement
        Dataset<Row> ryanAirFlights = flights
            .where("flight.operatingAirline.icaoCode = 'RYR'")
            .select("flight.originDate", "flight.flightStatus")
            .filter("flightStatus = 'X'").groupBy("originDate").count()
            .sort(col("count").desc()).limit(1);
        Row row = ryanAirFlights.first();
        String retVal = (String) row.get(0);
        return retVal;
    }

    /**
     * Task 5
     * Returns a dataset of Flight objects. The dataset only contains flights of the given airline with at least one
     * of the given status codes. Uses the given Dataset of Flights.
     *
     * @param flights            Dataset containing the Flight objects
     * @param airlineDisplayCode the display code of the airline
     * @param status1            the status code of the flight
     * @param status             more status codes
     * @return dataset of Flight objects matching the required fields
     */
    @Override
    public Dataset<Flight> flightsOfAirlineWithStatus(Dataset<Flight> flights, String airlineDisplayCode, String status1, String... status) {
        // TODO: Implement
        Dataset<Flight> queriedflights = flights
            .filter(r -> r.getAirlineDisplayCode().equals(airlineDisplayCode))
            .filter(r-> r.getFlightStatus().equals(status1) || java.util.Arrays.asList(status).contains(r.getFlightStatus()));
        return queriedflights;
    }

    /**
     * Task 6
     * Returns the average number of flights per day between the given timestamps (both included).
     * The timestamps are of the form 'hh:mm:ss'. Uses the given Dataset of Flights.
     * Hint: You only need to consider "scheduledTime" for this. Do not include flights with
     * empty "scheduledTime" field. You can assume that lowerLimit is always before or equal
     * to upperLimit.
     *
     * @param flights Dataset containing the arriving Flight objects
     * @param lowerLimit     start timestamp (included)
     * @param upperLimit     end timestamp (included)
     * @return average number of flights between the given timestamps (both included)
     */
    @Override
    public double avgNumberOfFlightsInWindow(Dataset<Flight> flights, String lowerLimit, String upperLimit) {
        // TODO: Implement
        Dataset<Flight> queriedflights = flights
            .filter(r ->
                (r.getScheduledTime() != "" && (isBefore(r.getScheduledTime(), upperLimit)
                    && isBefore(lowerLimit, r.getScheduledTime()))));
        long numFlights = queriedflights.count();
        Dataset<String> queriedFlights_string = queriedflights.map(f -> f.getScheduled().substring(0, 10), Encoders.STRING());
        Dataset<String> queriedFlights_string_distinct = queriedFlights_string.distinct();
        long numDays = queriedFlights_string_distinct.count();
        return (double) numFlights / (double) numDays;
    }

    /**
     * Returns true if the first timestamp is before or equal to the second timestamp. Both timestamps must match
     * the format 'hh:mm:ss'.
     * You can use this for Task 6. Alternatively use LocalTime or similar API (or your own custom method).
     *
     * @param before the first timestamp
     * @param after  the second timestamp
     * @return true if the first timestamp is before or equal to the second timestamp
     */
    private static boolean isBefore(String before, String after) {
        for (int i = 0; i < before.length(); i++) {
            char bef = before.charAt(i);
            char aft = after.charAt(i);
            if (bef == ':') continue;
            if (bef < aft) return true;
            if (bef > aft) return false;
        }

        return true;
    }
}
