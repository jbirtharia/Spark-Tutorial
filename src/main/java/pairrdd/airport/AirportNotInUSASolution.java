package pairrdd.airport;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import rdd.common.Utils;
import scala.Tuple2;

/**
 * @author JayendraB
 * Created on 27/11/21
 */
public class AirportNotInUSASolution {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("AirportNotInUSASolution").setMaster("local[*]");
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> rdd = context.textFile("in/airports.text");
        JavaPairRDD<String, String> pairRDD = rdd.mapToPair(airportPairFunction());
        JavaPairRDD filterPairRDD = pairRDD.filter(keyValue -> !keyValue._2().equals("\"United States\""));
        filterPairRDD.saveAsTextFile("out/airport_not_in_usa_pair_rdd");
    }

    private static PairFunction<String,String,String> airportPairFunction() {
        return s -> new Tuple2<>(
                s.split(Utils.COMMA_DELIMITER)[1],
                s.split(Utils.COMMA_DELIMITER)[3]
        );
    }
}
