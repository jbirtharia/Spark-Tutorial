package demo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

/**
 * @author JayendraB
 * Created on 10/01/23
 */
public class SparkTest {

    public static void main(String[] args) {

        SparkSession session = SparkSession.builder().appName("READ_PARQUET").master("local").getOrCreate();
        Dataset<Row> dataset = session.read()
                .parquet("/Users/jayendrabirtharia/Downloads/parquet-testing-master/data/alltypes_plain.parquet")
                //.filter(col("bigint_col").equalTo("10"))
                .filter(col("bool_col").equalTo(true))
        ;
        dataset.printSchema();
        dataset.show(false);
    }

}
