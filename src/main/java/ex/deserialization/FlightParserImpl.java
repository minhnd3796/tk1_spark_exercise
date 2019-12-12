package ex.deserialization;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import ex.deserialization.objects.Flight;
import ex.deserialization.objects.FlightObj;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class FlightParserImpl implements FlightParser, java.io.Serializable {
    private SparkSession sparkSession;

    public FlightParserImpl() {
        SparkConf conf = new SparkConf().setAppName("SparkExercise").setMaster("local");
        sparkSession = SparkSession.builder().config(conf).getOrCreate();
    }

    /**
     * Returns a Dataframe, containing all objects from the JSON files in the given path.
     *
     * @param path the path containing (only) the JSON files
     * @return Dataframe containing the input data
     */
    @Override
    public Dataset<Row> parseRows(String path) {
        return sparkSession.read().json(path);
    }

    /**
     * Deserializes the JSON lines to Flight objects. Each String in the given Dataset represents a JSON object.
     * Use Gson for this.
     * Hints: Apply a map function (mapPartitions) on the dataset, in which you use Gson to deserialize each line into objects of FlightObj.
     * Use GsonBuilder to register the type adapter "FlightAdapter" for FlightObj and to create a Gson object.
     *
     * @param path the path to parse the jsons from
     * @return dataset of Flight objects parsed from the JSON lines
     */
    @Override
    public Dataset<Flight> parseFlights(String path) {
        Dataset<String> lines = sparkSession.sqlContext().read().textFile(path);
        org.apache.spark.api.java.JavaRDD<String> rdd_string = lines.toJavaRDD();
        org.apache.spark.api.java.JavaRDD<Flight> rdd_flight =  rdd_string.mapPartitions(
            new org.apache.spark.api.java.function.FlatMapFunction<java.util.Iterator<String>, Flight>() {
                private static final long serialVersionUID = 1L;
                
                @Override
                public java.util.Iterator<Flight> call(java.util.Iterator<String> lines) throws Exception {
                    java.util.List<Flight> flights = new java.util.ArrayList<>();
                    GsonBuilder gsonBuilder = new GsonBuilder();
                    FlightAdapter flightAdapter = new FlightAdapter();
                    gsonBuilder.registerTypeAdapter(FlightObj.class, flightAdapter);
                    Gson customGson = gsonBuilder.create();
                    while (lines.hasNext()) {
                        String line = lines.next();
                        Flight flight = customGson.fromJson(line, FlightObj.class).getFlight();
                        System.out.println(flight.toString());
                        flights.add(flight);
                    }
                    return flights.iterator();
                  }
            }
        );
        return this.sparkSession.createDataset(rdd_flight.rdd(), Encoders.bean(Flight.class));
    }
}
