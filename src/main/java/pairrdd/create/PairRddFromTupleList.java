package pairrdd.create;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple1;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @author JayendraB
 * Created on 27/11/21
 */
public class PairRddFromTupleList {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("PairRddFromTupleList").setMaster("local[*]");
        JavaSparkContext context = new JavaSparkContext(conf);

        List<Tuple2<String,Integer>> tupples = Arrays.asList(
                new Tuple2<>("Sachin",43),new Tuple2<>("Rahul",53),
                new Tuple2<>("Rohit",23),new Tuple2<>("Suraj",16),new Tuple2<>("Adam",25)
        );

        JavaPairRDD<String, Integer> pairRDD = context.parallelizePairs(tupples);
        pairRDD.coalesce(1).saveAsTextFile("out/create_pair_rdd_from_tuple_list");
    }
}
