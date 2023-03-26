package rdd.nasaapacheweblogs;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author JayendraB
 * Created on 25/11/21
 */
public class SameHostsSolution {

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setAppName("sameHostsSolution").setMaster("local[*]");

        try(JavaSparkContext sc = new JavaSparkContext(conf)) {

            JavaRDD<String> julyFirstLogs = sc.textFile("in/nasa_19950701.tsv").map(line -> line.split("\t")[0]);
            JavaRDD<String> augustFirstLogs = sc.textFile("in/nasa_19950801.tsv").map(line -> line.split("\t")[0]);

            JavaRDD<String> commonLogs = julyFirstLogs.intersection(augustFirstLogs);

            JavaRDD<String> removeHeaders = commonLogs.filter(SameHostsSolution::isNotHeader);

            removeHeaders.saveAsTextFile("out/same_host_solution");
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    private static boolean isNotHeader(String line) {
        return !(line.startsWith("host") && line.contains("bytes"));
    }

}
