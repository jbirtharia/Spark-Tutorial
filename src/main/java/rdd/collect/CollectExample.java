package rdd.collect;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @author JayendraB
 * Created on 26/11/21
 */
public class CollectExample {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("collect").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> inputWords = Arrays.asList("spark", "hadoop", "spark", "hive", "pig", "cassandra", "hadoop");
        JavaRDD<String> wordRdd = sc.parallelize(inputWords);

        List<String> words = wordRdd.collect();
        words.forEach(System.out::println);

        Map<String, Long> collectByValue = wordRdd.countByValue();
        System.out.println("Count By Value : ");
        collectByValue.forEach((k,v)->{
            System.out.println(k + " : " + v);
        });

        System.out.println("Take Method : ");
        wordRdd.take(3).forEach(System.out::println);
    }
}
