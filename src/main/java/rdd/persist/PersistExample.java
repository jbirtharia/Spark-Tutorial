package rdd.persist;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import java.util.Arrays;

/**
 * @author JayendraB
 * Created on 26/11/21
 */
public class PersistExample {

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("persist").setMaster("local[*]");
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<Integer> numbers = context.parallelize(Arrays.asList(3,4,5,12,3,45));
        numbers.persist(StorageLevel.MEMORY_ONLY());

        numbers.reduce((x,y) -> x * y);
        System.out.println("Count : "+numbers.collect());
    }
}
