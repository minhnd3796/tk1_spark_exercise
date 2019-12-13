package ex;

import ex.deserialization.FlightParser;
import ex.deserialization.FlightParserImpl;
import ex.deserialization.objects.Flight;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.Tuple2;

public class Main {

    private static final String DEPARTURES = "./Fraport/*DEPARTURES*";
    private static final String ARRIVALS = "./Fraport/*ARRIVALS*";
    private static final String ALL_FLIGHTS = "./Fraport/";

    public static void main(String... args) {
        // comment out to enable log messages
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        AirportInfoImpl airportInfo = new AirportInfoImpl();
        FlightParser flightParser = new FlightParserImpl();

        exampleOutput(airportInfo, flightParser);
    }

    private static void exampleOutput(AirportInfoImpl airportInfo, FlightParser flightParser) {
        Dataset<Row> firstDay = flightParser.parseRows("./Fraport/*-08-08-*");
        airportInfo.sparkExample(firstDay);
        // System.out.println(airportInfo.ryanairStrike(flightParser.parseRows("./Fraport/*")));

        Dataset<Flight> flights = flightParser.parseFlights("./Fraport/*");
        // Dataset<Flight> queriedFlights = airportInfo.flightsOfAirlineWithStatus(flights, "DE", "X", "S", "");
        // queriedFlights.show(false);
        double count = airportInfo.avgNumberOfFlightsInWindow(flights, "00:00:00", "23:59:59");
        System.out.println(count);
    }
}
