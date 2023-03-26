package pairrdd.create;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author JayendraB
 * Created on 27/11/21
 */
public class PairRddFromRegularRdd {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("PairRddFromRegularRdd").setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        JavaRDD<String> rdd = sparkContext.parallelize(Arrays.asList("Sachin 23","Rohit 40","Adam 15","Suraj 78"));

        JavaPairRDD<String, Integer> pairRDD = rdd.mapToPair(getNameAndAgePair());
        pairRDD.coalesce(1).saveAsTextFile("out/pair_rdd_from_regular_rdd");
    }

    private static PairFunction<String,String,Integer> getNameAndAgePair() {
        return s -> new Tuple2<>(s.split(" ")[0],
                    Integer.valueOf(s.split(" ")[1]));
    }
}
