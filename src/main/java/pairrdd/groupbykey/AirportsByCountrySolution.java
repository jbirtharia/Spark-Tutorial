package pairrdd.groupbykey;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import rdd.common.Utils;
import scala.Tuple2;

import java.util.Map;

/**
 * @author JayendraB
 * Created on 12/12/21
 */
public class AirportsByCountrySolution {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("AirportsByCountrySolution").setMaster("local[*]");
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> rdd = context.textFile("in/airports.text");

        JavaPairRDD<String, String> airportByCountry = rdd.mapToPair( airport ->
                new Tuple2<>(airport.split(Utils.COMMA_DELIMITER)[3], airport.split(Utils.COMMA_DELIMITER)[1])
        );

        JavaPairRDD<String, Iterable<String>> groupByAirportName = airportByCountry.groupByKey();

        for (Map.Entry<String, Iterable<String>> airports : groupByAirportName.collectAsMap().entrySet()){
            System.out.println(airports.getKey() + " : " + airports.getValue());
        }
    }
}
