package pairrdd.reducebykey.wordcount;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

/**
 * @author JayendraB
 * Created on 12/12/21
 */
public class WordCount {

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("in/word_count.text");
        JavaRDD<String> wordRdd = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        JavaPairRDD<String, Integer> wordCount = wordRdd.mapToPair(x -> new Tuple2<>(x,1));
        JavaPairRDD<String, Integer> wordCounts = wordCount.reduceByKey((x, y) -> x+y);

        Map<String, Integer> worldCountsMap = wordCounts.collectAsMap();

        for (Map.Entry<String, Integer> wordCountPair : worldCountsMap.entrySet()) {
            System.out.println(wordCountPair.getKey() + " : " + wordCountPair.getValue());
        }
    }
}
