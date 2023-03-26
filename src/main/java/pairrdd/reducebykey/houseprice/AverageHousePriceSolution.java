package pairrdd.reducebykey.houseprice;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Map;

/**
 * @author JayendraB
 * Created on 11/12/21
 */
public class AverageHousePriceSolution {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("AverageHousePriceSolution");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("in/RealEstate.csv");
        JavaRDD<String> cleanedLines = lines.filter(line -> !line.contains("Bedrooms"));

        JavaPairRDD<String, AvgCount> housePricePairRdd = cleanedLines.mapToPair(
                line -> new Tuple2<>(line.split(",")[3],
                        new AvgCount(1, Double.parseDouble(line.split(",")[2]))));

        JavaPairRDD<String, AvgCount> housePriceTotal = housePricePairRdd.reduceByKey(
                (x,y) -> new AvgCount(x.getCount()+y.getCount(), x.getTotal()+y.getTotal())
        );

//        System.out.println("housePriceTotal: ");
//        for (Map.Entry<String, AvgCount> housePriceTotalPair : housePriceTotal.collectAsMap().entrySet()) {
//            System.out.println(housePriceTotalPair.getKey() + " : " + housePriceTotalPair.getValue());
//        }

        JavaPairRDD<String, Double> avgHousePrice = housePriceTotal.mapValues(x -> x.getTotal()/x.getCount());
        System.out.println("Average Housing Price : ");

        for (Map.Entry<String, Double> avgPrice : avgHousePrice.collectAsMap().entrySet()) {
            System.out.println(avgPrice.getKey() + " : " + avgPrice.getValue());
        }
    }
}
