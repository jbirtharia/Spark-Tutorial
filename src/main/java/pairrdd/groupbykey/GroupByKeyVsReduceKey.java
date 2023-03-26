package pairrdd.groupbykey;

import com.google.common.collect.Iterables;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @author JayendraB
 * Created on 12/12/21
 */
public class GroupByKeyVsReduceKey {

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("GroupByKeyVsReduceKey").setMaster("local[*]");
        JavaSparkContext context = new JavaSparkContext(conf);

        List<String> words = Arrays.asList("one", "two", "two", "three", "three", "three");
        JavaPairRDD<String, Integer> wordsRdd = context.parallelize(words)
                .mapToPair( word -> new Tuple2<>(word , 1));

        List<Tuple2<String, Integer>> wordCountWithReduceKey = wordsRdd.reduceByKey(
                (x, y) -> x + y).collect();
        System.out.println("Word Count By Reduce Key : "+wordCountWithReduceKey);

        List<Tuple2<String, Integer>> wordCountWithGroupBy = wordsRdd.groupByKey().mapValues(
                intIterable -> Iterables.size(intIterable)).collect();
        System.out.println("Word Count By Group By : "+wordCountWithGroupBy);

    }
}
