package rdd.sumofnumbersproblem;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * @author JayendraB
 * Created on 26/11/21
 */
public class SumOfNumbersSolution {

    public static void main(String[] args) throws Exception{
        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setAppName("sumOfNumbersSolution").setMaster("local[*]");

        try(JavaSparkContext sc = new JavaSparkContext(conf)) {

            JavaRDD<String> primeNums = sc.textFile("in/prime_nums.text")
                    .flatMap(line -> Arrays.asList(line.split("\\s+")).iterator());
            JavaRDD<Integer> primeNumsInInt = primeNums.map(num -> {
                if(null != num && !num.isEmpty()) {
                    num = num.trim();
                    return Integer.parseInt(num);
                }
                else {
                    return 0;
                }
            });

            Integer sum = primeNumsInInt.reduce((x,y)->x+y);

            System.out.println("Sum is : "+sum);
        }
    }
}
