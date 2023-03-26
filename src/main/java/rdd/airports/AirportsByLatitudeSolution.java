package rdd.airports;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import rdd.common.Utils;

/**
 * @author JayendraB
 * Created on 25/11/21
 */
public class AirportsByLatitudeSolution {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("airports").setMaster("local[2]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> airports = sc.textFile("in/airports.text");

        JavaRDD<String> airportsLargerLatitude = airports.filter(line ->
                Double.parseDouble(line.split(Utils.COMMA_DELIMITER)[6]) > 40);

        JavaRDD<String> airportsNameAndLatitude = airportsLargerLatitude.map(line -> {
                    String[] splits = line.split(Utils.COMMA_DELIMITER);
                    return StringUtils.join(new String[]{splits[1], splits[6]}, ",");
                }
        );
        airportsNameAndLatitude.saveAsTextFile("out/airports_with_larger_latitude");
    }
}
